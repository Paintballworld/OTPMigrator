package kz.greetgo.migration;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import kz.greetgo.connections.ConnectionPool;
import kz.greetgo.visualization.ProgressBar;
import kz.greetgo.visualization.ProgressPool;

public class MigrateTask implements Runnable {

  private ProgressPool progressPool;
  private ConcurrentLinkedQueue<String> tableNamesPool;
  private ConnectionPool connectionPool;
  private String tableName;
  private static List<String> errors = new ArrayList<>();
  private static List<String> success = new ArrayList<>();
  ProgressBar progressBar;

  public MigrateTask(ConnectionPool connectionPool, ProgressPool progressPool, ConcurrentLinkedQueue<String> tableNamesPool) {
    this.connectionPool = connectionPool;
    this.progressPool = progressPool;
    this.tableNamesPool = tableNamesPool;
  }

  @SuppressWarnings("StatementWithEmptyBody")
  @Override
  public void run() {
    progressBar = progressPool.createBar();
    while (iteration()) {}
  }

  private boolean iteration() {
    tableName = tableNamesPool.poll();
    if (tableName == null) return false;
    try (
      Connection source = connectionPool.borrowOracleConnection();
      Connection target = connectionPool.borrowPostgresConnection()
    ) {
      MigrateWorker worker = new MigrateWorker(source, target, progressBar, tableName);

      int migrated = worker.moveTableData();

      connectionPool.releaseOracleConnection(source);
      connectionPool.releasePostgresConnection(target);

      success.add(String.format("%-30s : %d", tableName, migrated));
    } catch (Exception e) {
      String hiddenMessage = "";
      if (e instanceof BatchUpdateException) {
        hiddenMessage = ((BatchUpdateException) e).getNextException().getMessage();
        errors.add(tableName + ":" + hiddenMessage);
      } else {
        errors.add(tableName);
        System.err.println("\n\t" + e.getMessage() + "\n");
      }
    }
    return true;
  }

  public static String getErrors() {
    String collect = errors.stream().sorted().collect(Collectors.joining("\n"));
    return !collect.isEmpty() ? collect : "<список пуст>";
  }

  public static String getSuccess() {
    String collect = success.stream().sorted().collect(Collectors.joining("\n"));
    return !collect.isEmpty() ? collect : "<список пуст>";
  }
}