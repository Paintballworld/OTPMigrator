package kz.greetgo.migration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import kz.greetgo.MigratorUtil;
import kz.greetgo.connections.ConnectionPool;
import static kz.greetgo.parameters.Parameters.*;
import kz.greetgo.visualization.ProgressPool;

public class Migrator {

  private List<String> excludePatterns = new ArrayList<>();
  private boolean excludesInitialized = false;

  private static void testOnStartup() throws Exception {
    MigratorUtil.printTestTable();
    MigratorUtil.testMigration();
  }

  public void mainMigrationProcess() throws Exception {

    Long startTime = System.currentTimeMillis();

    System.out.println("");

    System.out.println("O - Oracle source, \nB - Application buffer, \nP - Postgres target\n");
    ConcurrentLinkedQueue<String> tableNamesQueue = new ConcurrentLinkedQueue<>();
    ProgressPool progressPool = new ProgressPool();
    Thread progressThread = new Thread(progressPool);
    try (
      ConnectionPool connectionPool = new ConnectionPool(MAX_THREAD_COUNT)
    ) {
      Connection unPooledOracleConnection = connectionPool.getUnPooledOracleConnection();
      Connection unPooledPostgresConnection = connectionPool.getUnPooledPostgresConnection();

      if (NEED_CLEAR)
        clearAllTables(unPooledPostgresConnection);

      System.err.print("\rПолучение списка имен таблиц...");
      fillTableNamesFromOracle(GET_TABLE_NAMES_QUERY, unPooledOracleConnection, tableNamesQueue);
      System.err.println("\rКоличество таблиц для миграции: " + tableNamesQueue.size());

      progressThread.start();
      realMigration(tableNamesQueue, progressPool, connectionPool);

    } finally {
      progressPool.finish();
      progressThread.interrupt();

      System.out.println("\n\tУспешно смигрированные таблицы:");
      System.out.println(MigrateTask.getSuccess());

      System.err.println("\r\n\n\tДанные не промигрированы:");
      System.err.println(MigrateTask.getErrors());

      Long elapsed = System.currentTimeMillis() - startTime;
      System.out.println("\n\n\n\tЗатрачено времени: " + MigratorUtil.getStrRepresentationOfTime(elapsed, -1));
    }
  }

  private void clearAllTables(Connection connection) throws SQLException, IOException {
    ConcurrentLinkedQueue<String> tableNamesToClean = new ConcurrentLinkedQueue<>();
    fillTableNamesFromOracle(CLEAN_TABLES_QUERY, connection, tableNamesToClean);
    try (
      Statement st = connection.createStatement()
    ) {
      while (tableNamesToClean.size() > 0) {
        String tableName = tableNamesToClean.poll();
        System.err.print("\r Чистим таблицу " + tableName);
        try {
          st.executeUpdate("delete from " + tableName);
        } catch (SQLException e) {
          System.err.println("\r Возвращаем в очередь " + tableName);
          tableNamesToClean.add(tableName);
        }
      }
    }
  }

  private void fakeMigration(Collection<String> tableNamesQueue) {
    tableNamesQueue.forEach(System.err::println);
  }

  private void fillTableNamesFromOracle(String query, Connection connection, Collection<String> tableNamesQueue) throws SQLException {
    List<String> excludeTableNameList = Arrays.stream(excludeTables.split(",")).map(String::trim).collect(Collectors.toList());

    try (
      PreparedStatement st = connection.prepareStatement(query)
    ) {
      ResultSet resultSet = st.executeQuery();
      if (resultSet == null)
        throw new RuntimeException(String.format("Нет данных при выборке '%s'", GET_TABLE_NAMES_QUERY));

      while (resultSet.next()) {
        String tableName = resultSet.getString(1);

        if (!excludeTableNameList.contains(tableName) && !needToBeExcluded(tableName))
          tableNamesQueue.add(tableName);
      }
    }
  }

  private void initializeExclude() {
    if (TABLE_NAME_REGEX_TO_EXCLUDE != null && !TABLE_NAME_REGEX_TO_EXCLUDE.isEmpty())
      excludePatterns.add(TABLE_NAME_REGEX_TO_EXCLUDE);
    if (FILE_REGEX_TO_EXCLUDE != null && !FILE_REGEX_TO_EXCLUDE.isEmpty()) {
      File file = new File(FILE_REGEX_TO_EXCLUDE);
      if (!file.exists())
        throw new RuntimeException(String.format("Не удается найти файл %s, не используйте параметр %s или уточните существующее местоположение файла", FILE_REGEX_TO_EXCLUDE, REGEX_EX_FILE));

      try (
        BufferedReader reader = new BufferedReader(new FileReader(file))
      ) {
        String line;
        while ((line = reader.readLine()) != null) {
          excludePatterns.add(line);
        }
      } catch (IOException e) {
        throw new RuntimeException(String.format("Не удается прочитать файл %s, не используйте параметр %s или исправьте существующее местоположение файла", FILE_REGEX_TO_EXCLUDE, REGEX_EX_FILE));
      }
    }
    excludesInitialized = false;
  }

  private boolean needToBeExcluded(String tableName) {
    if (!excludesInitialized)
      initializeExclude();
    return excludePatterns.parallelStream()
      .map(Pattern::compile)
      .map(o -> o.matcher(tableName))
      .map(Matcher::matches)
      .filter(o -> o)
      .findAny()
      .orElse(false);
  }

  private void realMigration(ConcurrentLinkedQueue<String> tableNamesQueue, ProgressPool progressPool, ConnectionPool connectionPool) throws InterruptedException {
    List<Thread> allThreads = new ArrayList<>();
    for (int i = 0; i < MAX_THREAD_COUNT; i++) {
      allThreads.add(new Thread(new MigrateTask(connectionPool, progressPool, tableNamesQueue)));
    }
    for (int i = 0; i < MAX_THREAD_COUNT; i++) {
      allThreads.get(i).start();
    }
    for (int i = 0; i < MAX_THREAD_COUNT; i++) {
      allThreads.get(i).join();
    }
  }

}