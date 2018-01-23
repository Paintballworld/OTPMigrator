package kz.greetgo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import kz.greetgo.connections.ConnectionPool;
import kz.greetgo.visualization.ProgressPool;

public class Main {

  public static final String APP_DIR = "kaspiptp.d";
  public static final String REGEX_EX = "--regex-ex";
  public static final String CLEAN_TABLES_QUERY = "SELECT table_name FROM information_schema.tables WHERE table_schema='ptp'";
  public static String GET_TABLE_NAMES_QUERY = "select distinct table_name from user_tables order by table_name";
  public static String POSTGRES_CONFIG_FILE = System.getProperty("user.home") + "/" + APP_DIR + "/postgres.properties";
  public static String ORACLE_CONFIG_FILE = System.getProperty("user.home") + "/" + APP_DIR + "/oracle.properties";
  public static int MAX_DATA_COUNT = 1_000_000;
  public static int MAX_BATCH_SIZE = 50_000;
  public static String TABLE_NAME_REGEX_TO_EXCLUDE = null;
  public static int MAX_THREAD_COUNT = 4;
  private static boolean NEED_CLEAR = false;

  public static String excludeTables = "DATABASECHANGELOG, DATABASECHANGELOGLOCK";

  private static final String POSTGRES_PROP = "--postgres-prop";
  private static final String ORACLE_PROP = "--oracle-prop";
  private static final String MOCK_COUNT = "--mock-count";
  private static final String BATCH_SIZE = "--batch-size";
  private static final String HELP_SHORT = "-h";
  private static final String HELP = "--help";
  private static final String TEST = "--test";
  private static final String TEST_SHORT = "-t";
  private static final String CLEAR_POSTGRES_DATA = "--clear-postgres";

  private static Map<String, String> attributes = new HashMap<>();

  public static void main(String[] args) throws Exception {
    prepareArguments(args);
    resolveArguments();

    //
    // Migrate process initialization
    //

    Long startTime = System.currentTimeMillis();

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

//      fakeMigration(tableNamesQueue);
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

  private static void clearAllTables(Connection connection) throws SQLException {
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

  private static void realMigration(ConcurrentLinkedQueue<String> tableNamesQueue, ProgressPool progressPool, ConnectionPool connectionPool) throws InterruptedException {
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

  private static void fakeMigration(Collection<String> tableNamesQueue) {
    tableNamesQueue.forEach(System.err::println);
  }

  private static void fillTableNamesFromOracle(String query, Connection connection, Collection<String> tableNamesQueue) throws SQLException {
    List<String> excludeTableNameList = Arrays.stream(excludeTables.split(",")).map(String::trim).collect(Collectors.toList());

    try (
      PreparedStatement st = connection.prepareStatement(query)
    ) {
      ResultSet resultSet = st.executeQuery();
      if (resultSet == null)
        throw new RuntimeException(String.format("Нет данных при выборке '%s'", GET_TABLE_NAMES_QUERY));

      while (resultSet.next()) {
        String tableName = resultSet.getString(1);
        if (!excludeTableNameList.contains(tableName))
          tableNamesQueue.add(tableName);
      }
    }
  }

  private static void resolveArguments() throws Exception {

    if (argument(HELP) || argument(HELP_SHORT))
      help();

    Integer argDataCount = intArg(MOCK_COUNT);
    MAX_DATA_COUNT = argDataCount != null ? argDataCount : MAX_DATA_COUNT;

    Integer argBatchSize = intArg(BATCH_SIZE);
    MAX_BATCH_SIZE = argBatchSize != null ? argBatchSize : MAX_BATCH_SIZE;


    String argPostgresFile = argValue(POSTGRES_PROP);
    POSTGRES_CONFIG_FILE = argPostgresFile != null && !argPostgresFile.isEmpty() ? argPostgresFile : POSTGRES_CONFIG_FILE;
    System.out.println("Используется параметры доступа к ДБ postgres из " + POSTGRES_CONFIG_FILE);

    String argOracleFile = argValue(ORACLE_PROP);
    ORACLE_CONFIG_FILE = argOracleFile != null && !argOracleFile.isEmpty() ? argOracleFile : ORACLE_CONFIG_FILE;
    System.out.println("Используется параметры доступа к ДБ oracle из " + ORACLE_CONFIG_FILE);

    if (argument(TEST) || argument(TEST_SHORT))
      testOnStartup();

    if (argument(CLEAR_POSTGRES_DATA))
      NEED_CLEAR = true;

    String argRegex = argValue(REGEX_EX);
    TABLE_NAME_REGEX_TO_EXCLUDE = argRegex != null && !argRegex.isEmpty() ? argRegex : null;

    if (TABLE_NAME_REGEX_TO_EXCLUDE != null)
      System.out.println("При миграции будет использоваться исключающая маска для названий: " + TABLE_NAME_REGEX_TO_EXCLUDE);
  }

  private static void testOnStartup() throws Exception {
    MigratorUtil.printTable();
    MigratorUtil.testMigration();
  }

  private static boolean argument(String key) {
    return argValue(key) != null;
  }

  private static Integer intArg(String attrKey) {
    String val = argValue(attrKey);
    if (val != null && val.isEmpty())
      throw new IllegalArgumentException("При указании ключа " + attrKey + " не указано числовое значение");
    return val != null ? Integer.parseInt(val) : null;
  }

  private static String argValue(String attrKey) {
    if (attributes.isEmpty()) return null;
    return attributes.get(attrKey);
  }

  private static void prepareArguments(String[] args) {
    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith("-")) {
        String val = i + 1 < args.length ? args[++i] : "";
        attributes.put(args[i], val);
      }
    }
  }

  private static void help() {
    StringBuilder sb = new StringBuilder("Помощь:\n");
    sb.append(String.format("\t %-7s, %-7s - %s \n", HELP, HELP_SHORT, "помощь"));
    sb.append(String.format("\t %-7s, %-7s - %s \n", TEST, TEST_SHORT, "запуск только теста по миграции"));
    sb.append(String.format("\t %-16s - %s \n", MOCK_COUNT, "количество данных для тестов (по умолчанию - 1 000 000)"));
    sb.append(String.format("\t %-16s - %s \n", BATCH_SIZE, "размер батча записи (по умолчанию - 50 000)"));
    sb.append(String.format("\t %-16s - %s \n", POSTGRES_PROP, "полный путь к файлу с настройками доступа к БД postgres"));
    sb.append(String.format("\t %-16s - %s \n", ORACLE_PROP, "полный путь к файлу с настройками доступа к БД oracle"));
    sb.append(String.format("\t %-16s - %s \n", REGEX_EX, "регулярное выражение (маска) для исключения названий мигрируемых объектов"));
    sb.append(String.format("\t %-16s - %s \n", CLEAR_POSTGRES_DATA, "перед миграцией произвести чистку всех таблиц"));

    System.err.println(sb.toString());
    System.exit(0);
  }




}
