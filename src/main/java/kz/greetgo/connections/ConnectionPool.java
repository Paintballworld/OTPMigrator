package kz.greetgo.connections;

import static kz.greetgo.parameters.Parameters.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import kz.greetgo.conf.ConfData;

public class ConnectionPool implements AutoCloseable {

  private static final int DEFAULT_INITIAL_POOL_SIZE = 5;
  private Set<Connection> allConnections = new HashSet<>();
  private ConcurrentLinkedQueue<Connection> freeOracleConnections = new ConcurrentLinkedQueue<>();
  private ConcurrentLinkedQueue<Connection> freePostgresConnections = new ConcurrentLinkedQueue<>();
  private ConfData oracleConf;
  private ConfData postgresConf;
  private boolean isInitialized = false;

  public Connection getUnPooledOracleConnection() throws Exception {
    return getOracleConnect();
  }

  public Connection getUnPooledPostgresConnection() throws Exception {
    return getPostgresConnect();
  }

  public ConnectionPool(int initialPoolSize) throws Exception {
    createPool(initialPoolSize);
  }

  public ConnectionPool() throws Exception {
    createPool(DEFAULT_INITIAL_POOL_SIZE);
  }

  public synchronized Connection borrowOracleConnection() throws Exception {
    Connection connection = freeOracleConnections.poll();
    if (connection != null && !connection.isClosed()) return connection;
    return getOracleConnect();
  }

  public synchronized Connection borrowPostgresConnection() throws Exception {
    Connection connection = freePostgresConnections.poll();
    if (connection != null && !connection.isClosed()) return connection;
    return getPostgresConnect();
  }

  public void releaseOracleConnection(Connection oracle) {
    freeOracleConnections.add(oracle);
  }

  public void releasePostgresConnection(Connection postgres) {
    freePostgresConnections.add(postgres);
  }

  private void createPool(int initialPoolSize) throws Exception {
    init();
    for (int i = 0; i < initialPoolSize; i++) {
      freeOracleConnections.add(getOracleConnect());
      freePostgresConnections.add(getPostgresConnect());
    }
  }

  private Connection getPostgresConnect() throws Exception {
    Connection postgres =  DriverManager.getConnection("jdbc:postgresql://"//
        + postgresConf.str("db.host") + ":" + postgresConf.str("db.port") + "/" + postgresConf.str("db.sid"),
      postgresConf.str("db.username"), postgresConf.str("db.password"));
    if (postgres != null) {
//      System.err.print("\rСоединение с postgres успешно установлено");
      Thread.sleep(220);
    } else {
      System.out.println("\rНет соединения с postgres");
      throw new RuntimeException("No Postgres connection");
    }
    postgres.setAutoCommit(false);
    allConnections.add(postgres);
    return postgres;
  }

  private void init() throws Exception {
    if (isInitialized) return;
    oracleConf = new ConfData();
    oracleConf.readFromFile(ORACLE_CONFIG_FILE);
    Class.forName("oracle.jdbc.driver.OracleDriver");
    postgresConf = new ConfData();
    postgresConf.readFromFile(POSTGRES_CONFIG_FILE);
    Class.forName("org.postgresql.Driver");
    isInitialized = true;
  }

  private Connection getOracleConnect() throws Exception {
    Connection oracle = DriverManager.getConnection("jdbc:oracle:thin:@"//
        + oracleConf.str("db.host") + ":" + oracleConf.str("db.port") + ":" + oracleConf.str("db.sid"),
      oracleConf.str("db.username"), oracleConf.str("db.password"));
    if (oracle != null) {
//      System.err.print("\rСоединение с oraсle успешно установлено");
      Thread.sleep(220);
    } else {
      System.out.println("\rНет соединения с oracle");
      throw new RuntimeException("No Oracle connection");
    }
    allConnections.add(oracle);
    return oracle;
  }

  @Override
  public void close() throws Exception {
    for (Connection connection : allConnections) {
      if (!connection.isClosed())
        connection.close();
    }
  }

}