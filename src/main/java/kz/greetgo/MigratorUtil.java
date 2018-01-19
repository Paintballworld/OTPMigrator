package kz.greetgo;

import static kz.greetgo.Main.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import kz.greetgo.connections.ConnectionPool;

public class MigratorUtil {

  public static final String CHECK_COLUMN_QUERY = "select * from testtable";
  private static ConnectionPool pool;

  public static void testMigration() throws Exception {
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    initPool();

    Connection oracle = pool.borrowOracleConnection();

    Connection postgres = pool.borrowPostgresConnection();

    recreateBothTables(oracle, postgres);

    System.out.print("\rГенерация исходных данных");
    populateSourceTable(oracle);

    System.out.print("\rНачало миграции");
    long start = System.currentTimeMillis();
    long myTime = migrate(oracle, postgres);
    long diff = System.currentTimeMillis() - start;

    System.out.println("\rВсего данных промигрировано " + MAX_DATA_COUNT);
    System.out.println("Процесс миграции занял <" + diff + "> мс");
    System.out.println("Процесс записи занял <" + myTime + "> мс");

    oracle.close();
    postgres.close();
  }

  private static void initPool() throws Exception {
    if (pool != null) return;
    pool = new ConnectionPool();
  }

  public static void checkColumns() throws Exception {
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    initPool();

    Connection oracle = pool.borrowOracleConnection();
    recreateTable(oracle, false);

    try (
      PreparedStatement st = oracle.prepareStatement(CHECK_COLUMN_QUERY)
    ) {
      ResultSet resultSet = st.executeQuery();

      Map<Integer, String> resultSetTypeMap = getResultSetTypeMap(resultSet);

      System.out.println();

      for (Map.Entry<Integer, String> entry : resultSetTypeMap.entrySet()) {

        System.out.println("Column " + entry.getKey() + ":");
        System.out.println(entry.getValue());
      }

    }
  }



  public static void recreateBothTables(Connection oracle, Connection postgres) throws SQLException {
    System.out.print("\rRecreating tables");
    recreateTable(oracle, false);
    recreateTable(postgres, true);
  }


  public static void recreateTable(Connection target, boolean isPostgres) throws SQLException {
    try {
      drop(target);
      clear(target);
    } catch (Exception e) {
      System.out.print("\rСоздание временной таблицы для " + (isPostgres ? "postgres" : "oracle"));
      String sql = isPostgres ?
        "create table testtable (\n" +
          "val1 varchar(300),\n" +
          "val2 decimal,\n" +
          "val3 timestamp default now()\n" +
          ") \n" :
        "create table testtable (\n" +
          "val1 varchar2(300),\n" +
          "val2 INTEGER,\n" +
          "val3 timestamp default systimestamp\n" +
          ") \n";
      executeSql(target, sql);
      System.out.print("\rВыполнено");
    }
  }

  public static void drop(Connection connection) throws SQLException {
    String sql = "drop table testtable";
    executeSql(connection, sql);
  }

  public static void clear(Connection connection) throws SQLException {
    String sql = "delete from testtable";
    executeSql(connection, sql);
  }

  public static void executeSql(Connection connection, String sql) throws SQLException {
    try (
      PreparedStatement st = connection.prepareStatement(sql)
    ) {
      st.executeUpdate();
    }
  }

  public static long migrate(Connection source, Connection target) {
    long result = 0;
    String selectSQL = "select * from testtable";
    String insertSQL = "insert into testtable (val1, val2, val3) values (?, ?, ?)";
    try (
      PreparedStatement sourcePS = source.prepareStatement(selectSQL);
      PreparedStatement targetPS = target.prepareStatement(insertSQL)
    ) {
      ResultSet sourceResultSet = sourcePS.executeQuery();

      target.setAutoCommit(false);

      int count = 0;
      int batchCounter = 0;
      while (sourceResultSet.next()) {

        targetPS.setString(1, sourceResultSet.getString(1));
        targetPS.setInt(2, sourceResultSet.getInt(2));
        targetPS.setTimestamp(3, sourceResultSet.getTimestamp(3));

        targetPS.addBatch();

        if (++count == MAX_BATCH_SIZE) {
          System.out.print("\rЗаписываем " + (++batchCounter) + "-й батч ...");
          long start = System.currentTimeMillis();
          targetPS.executeBatch();
          target.commit();
          result += System.currentTimeMillis() - start;
          count = 0;
          System.out.print("\r" + batchCounter + " Чтение данных...");
        }
      }

      if (count > 0) {

        targetPS.executeBatch();
        target.commit();
      }

      System.out.print("\r");
      target.setAutoCommit(true);

    } catch (SQLException e) {
      e.printStackTrace();
    }
    return result;
  }

  public static void populateSourceTable(Connection connection) {
    String sql = "insert into testtable (val1, val2, val3) values (?, ?, ?)";
    try (
      PreparedStatement st = connection.prepareStatement(sql);
    ) {
      connection.setAutoCommit(false);

      int count = 0;
      while (count++ <= MAX_DATA_COUNT) {
        st.setString(1, "value#" + count);
        st.setInt(2, count);
        st.setTimestamp(3, new Timestamp((new Date()).getTime()));

        st.addBatch();

        if (count % MAX_BATCH_SIZE == 0 || count == MAX_DATA_COUNT) {
          st.executeBatch();
          connection.commit();
        }

      }

      connection.setAutoCommit(true);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public static void printTable() {
    System.err.println("    ╔════════════════════════════════════════════╗    ");
    System.err.println("    ║                                            ║    ");
    System.err.println("    ║        Проверка на готовность БД к         ║    ");
    System.err.println("    ║             процессу миграции              ║    ");
    System.err.println("    ║         просредством мигрирования          ║    ");
    System.err.println("    ║              тестовых данных               ║    ");
    System.err.println("    ║                                            ║    ");
    System.err.println("    ╚════════════════════════════════════════════╝    ");
  }

  public static Map<Integer, String> getResultSetTypeMap(ResultSet resultSet) throws SQLException {
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

    Map<Integer, String> resultMap = new HashMap<>();
    int columnCount = resultSetMetaData.getColumnCount();
    for (int i = 1; i <= columnCount; i++) {
      resultMap.put(i,
        String.format("%20s: %s\n%20s: %s\n%20s: %s\n%20s: %s\n%20s: %s\n",
          "getColumnLabel", resultSetMetaData.getColumnLabel(i),
          "getColumnType", resultSetMetaData.getColumnType(i),
          "getColumnType", resultSetMetaData.getColumnType(i),
          "getPrecision", resultSetMetaData.getPrecision(i),
          "getColumnTypeName", resultSetMetaData.getColumnTypeName(i)));
    }
    return resultMap;
  }

  public static void main(String[] args) throws Exception {
    checkColumns();
  }


}