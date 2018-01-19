package kz.greetgo;

import java.sql.Connection;
import java.util.concurrent.ConcurrentLinkedQueue;
import kz.greetgo.connections.ConnectionPool;
import kz.greetgo.visualization.ProgressBar;
import kz.greetgo.visualization.ProgressPool;

public class MigrateTask implements Runnable {

  private ProgressPool progressPool;
  private ConcurrentLinkedQueue<String> tableNamesPool;
  private ConnectionPool connectionPool;

  public MigrateTask(ConnectionPool connectionPool, ProgressPool progressPool, ConcurrentLinkedQueue<String> tableNamesPool) {
    this.connectionPool = connectionPool;
    this.progressPool = progressPool;
    this.tableNamesPool = tableNamesPool;
  }

  @Override
  public void run() {
    ProgressBar progressBar = progressPool.getFreeProgressBar();
    String tableName = tableNamesPool.poll();
    try (
      Connection source = connectionPool.borrowOracleConnection();
      Connection target = connectionPool.borrowPostgresConnection()
    ) {
      MigrateWorker worker = new MigrateWorker(source, target, progressBar, tableName);

      worker.moveTableData();

      connectionPool.releaseOracleConnection(source);
      connectionPool.releasePostgresConnection(target);

    } catch (Exception e) {
      e.printStackTrace();
    }


  }
}