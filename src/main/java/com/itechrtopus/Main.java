package com.itechrtopus;

import kz.greetgo.conf.ConfData;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Date;

public class Main {

  private static final String APP_DIR = "kaspiptp.d";
  public static final int MAX_DATA_COUNT = 1_000_000;
  public static final int MAX_BATCH_DATA = 50_000;

  public static void main(String[] args) throws Exception {

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    Connection oracle = getOracleConnection();

    if (oracle != null)
      System.out.println("Successfully established Oracle connection");
    else {
      System.out.println("No Oracle connection");
      throw new RuntimeException("No Oracle connection");
    }
    Connection postgres = getPostgresConnection();
    if (postgres != null)
      System.out.println("Successfully established Postgres connection");
    else {
      System.out.println("No Postgres connection");
      throw new RuntimeException("No Postgres connection");
    }

    Connection source = oracle;
    Connection target = postgres;

    System.out.println("Clearing tables");
    try {
      clear(oracle);
    } catch (Exception e) {
      System.out.println("creating table for oracle");
      executeSql(oracle, "create table testtable (\n" +
        "val1 varchar(300),\n" +
        "val2 decimal,\n" +
        "val3 timestamp default systimestamp\n" +
        ") \n");
    }
    clear(postgres);

    System.out.println("Generating data for source testTable");
    populateSourceTable(source);

    System.out.println("Start to migrate data");
    long start = System.currentTimeMillis();
    long myTime = migrate(source, target);
    long diff = System.currentTimeMillis() - start;


    System.out.println("Migrate took <" + diff + "> ms");
    System.out.println("Процесс записи занял <" + myTime + "> ms");

    oracle.close();
    postgres.close();


  }

  private static void clear(Connection connection) throws SQLException {
    String sql = "delete from testtable";
    executeSql(connection, sql);
  }

  private static void executeSql(Connection connection, String sql) throws SQLException {
    try (
      PreparedStatement st = connection.prepareStatement(sql)
    ) {
      st.executeUpdate();
    }
  }

  private static long  migrate(Connection source, Connection target) {
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

        if (++count == MAX_BATCH_DATA) {
          System.out.print("Executing " + (++batchCounter) + "'th batch ...");
          long start = System.currentTimeMillis();
          targetPS.executeBatch();
          target.commit();
          result += System.currentTimeMillis() - start;
          count = 0;
          System.out.println(" Finished.");

        }
      }

      if (count > 0) {
        targetPS.executeBatch();
        target.commit();
      }

      target.setAutoCommit(true);


    } catch (SQLException e) {
      e.printStackTrace();
    }
    return result;
  }

  private static void populateSourceTable(Connection connection) {
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

        if (count % MAX_BATCH_DATA  == 0 || count == MAX_DATA_COUNT) {
          st.executeBatch();
          connection.commit();
        }

      }

      connection.setAutoCommit(true);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public static Connection getOracleConnection() throws Exception, ClassNotFoundException, SQLException {
    ConfData cd = new ConfData();
    cd.readFromFile(System.getProperty("user.home") + "/" + APP_DIR + "/oracle.properties");

    Class.forName("oracle.jdbc.driver.OracleDriver");

    return DriverManager.getConnection("jdbc:oracle:thin:@"//
        + cd.str("db.host") + ":" + cd.str("db.port") + ":" + cd.str("db.sid"),
      cd.str("db.username"), cd.str("db.password"));
  }

  public static Connection getPostgresConnection() throws Exception, ClassNotFoundException, SQLException {
    ConfData cd = new ConfData();
    cd.readFromFile(System.getProperty("user.home") + "/" + APP_DIR + "/postgres.properties");

    Class.forName("org.postgresql.Driver");

    return DriverManager.getConnection("jdbc:postgresql://"//
        + cd.str("db.host") + ":" + cd.str("db.port") + ":" + cd.str("db.sid"),
      cd.str("db.username"), cd.str("db.password"));
  }


}
