package kz.greetgo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import kz.greetgo.visualization.MigrationStatus;
import kz.greetgo.visualization.ProgressBar;

public class MigrateWorker {

  public static final String TARGET_INSERT_QUERY = "insert into {{table_name}} ({{column_names}}) values ({{question_marks}})";
  private static final String SOURCE_SELECT_QUERY = "select * from {{table_name}} ";
  private static final String DELETE_TARGET_TABLE_QUERY = "delete from {{table_name}}";
  private static final String DROP_TARGET_TABLE_QUERY = "drop table {{table_name}}";
  private static final String SELECT_COUNT_QUERY = "select count(*) from {{table_name}}";
  public static final int PROGRESS_BAR_SIZE = 10;
  public static final int PROCESS_NOT_INITIALIZED = 0;
  public static final int PROCESS_STOPPED = 1;
  public static final int QUERYING = 3;
  public static final int EXECUTING_BATCH = 4;

  private Connection source;
  private Connection target;
  private ProgressBar bar;
  private String tableName;

  public MigrateWorker(Connection source, Connection target, ProgressBar bar, String tableName) throws SQLException {
    this.source = source;
    this.target = target;
    this.bar = bar;
    this.tableName = tableName;
  }

  public void deleteFromTargetTable(String tableName) throws SQLException {
    executeSql(DELETE_TARGET_TABLE_QUERY.replace("{{table_name}}", tableName), target);
  }

  public void dropTargetTable(String tableName) throws SQLException {
    executeSql(DROP_TARGET_TABLE_QUERY.replace("{{table_name}}", tableName), target);
  }

  public void  moveTableData() throws Exception {
    int result = 0;
    bar.resetWithNewTable(tableName);

    int dataCount = retrieveDataCount(SELECT_COUNT_QUERY.replace("{{table_name}}", tableName), source);

    bar.start(dataCount);


    String selectSQL = SOURCE_SELECT_QUERY.replace("{{table_name}}", tableName);
    AtomicInteger currentCount = new AtomicInteger(0);

    try (
      Statement sourceSelectStatement = source.createStatement()
    ) {
      ResultSet sourceSet = sourceSelectStatement.executeQuery(selectSQL);

      if (sourceSet == null || !sourceSet.next()) return;
      try (
        ColumnUtilWorker columnUtilWorker = new ColumnUtilWorker(sourceSet, tableName, target)
      ) {
        boolean needToExit = false;
        bar.setStatus(MigrationStatus.READING);
        while (!needToExit) {
          columnUtilWorker.addBatch();
          needToExit = !sourceSet.next();
          bar.setCurrent(++result);
          if ((result % Main.MAX_BATCH_SIZE == 0) || needToExit) {
            bar.setStatus(MigrationStatus.WRITING);
            columnUtilWorker.executeBatch();
            target.commit();
            bar.setStatus(MigrationStatus.READING);
          }
          currentCount.set(result);
        }
      } catch (Exception debug) {
        debug.printStackTrace();
      }
    }
    bar.setStatus(MigrationStatus.RELEASED);

  }

  private int retrieveDataCount(String sql, Connection connection) throws SQLException {
    try (
      Statement st = connection.createStatement()
    ) {
      ResultSet resultSet = st.executeQuery(sql);

      if (resultSet != null && resultSet.next())
        return resultSet.getInt(1);
    }
    return -1;
  }


  private void executeSql(String sql, Connection connection) throws SQLException {
    try (
      Statement st = connection.createStatement()
    ) {
      st.executeUpdate(sql);
      if (!connection.getAutoCommit())
        connection.commit();
    }
  }
}