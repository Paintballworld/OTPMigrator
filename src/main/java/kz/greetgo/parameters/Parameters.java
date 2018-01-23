package kz.greetgo.parameters;

import java.util.HashMap;
import java.util.Map;

public class Parameters {

  public static final String APP_DIR = "kaspiptp.d";
  public static final String REGEX_EX = "--regex-exclude";
  public static final String REGEX_EX_FILE = "--regex-file";
  public static final String POSTGRES_PROP = "--postgres-prop";
  public static final String ORACLE_PROP = "--oracle-prop";
  public static final String MOCK_COUNT = "--mock-count";
  public static final String BATCH_SIZE = "--batch-size";
  public static final String HELP_SHORT = "-h";
  public static final String HELP = "--help";
  public static final String TEST = "--test";
  public static final String TEST_SHORT = "-t";
  public static final String CLEAR_POSTGRES_DATA = "--clear-postgres";

  public static String CLEAN_TABLES_QUERY = "SELECT table_name FROM information_schema.tables WHERE table_schema='ptp'";
  public static String GET_TABLE_NAMES_QUERY = "select distinct table_name from user_tables order by table_name";
  public static String POSTGRES_CONFIG_FILE = System.getProperty("user.home") + "/" + APP_DIR + "/postgres.properties";
  public static String ORACLE_CONFIG_FILE = System.getProperty("user.home") + "/" + APP_DIR + "/oracle.properties";
  public static int MAX_DATA_COUNT = 1_000_000;
  public static int MAX_BATCH_SIZE = 50_000;
  public static String TABLE_NAME_REGEX_TO_EXCLUDE = "";
  public static String FILE_REGEX_TO_EXCLUDE = "";
  public static int MAX_THREAD_COUNT = 4;
  public static boolean NEED_CLEAR = false;
  public static boolean NEED_TEST = false;
  public static String excludeTables = "DATABASECHANGELOG, DATABASECHANGELOGLOCK";
  public static Map<String, String> attributes = new HashMap<>();

  public static void resolveArguments() throws Exception {

    if (argument(HELP) || argument(HELP_SHORT))
      help();

    Integer argDataCount = intArg(MOCK_COUNT);
    MAX_DATA_COUNT = argDataCount != null ? argDataCount : MAX_DATA_COUNT;

    Integer argBatchSize = intArg(BATCH_SIZE);
    MAX_BATCH_SIZE = argBatchSize != null ? argBatchSize : MAX_BATCH_SIZE;

    Integer argThreadCount = intArg(BATCH_SIZE);
    MAX_THREAD_COUNT = argThreadCount != null ? argThreadCount : MAX_THREAD_COUNT;

    String argPostgresFile = argValue(POSTGRES_PROP);
    POSTGRES_CONFIG_FILE = argPostgresFile != null && !argPostgresFile.isEmpty() ? argPostgresFile : POSTGRES_CONFIG_FILE;
    System.out.println("Используется параметры доступа к ДБ postgres из " + POSTGRES_CONFIG_FILE);

    String argOracleFile = argValue(ORACLE_PROP);
    ORACLE_CONFIG_FILE = argOracleFile != null && !argOracleFile.isEmpty() ? argOracleFile : ORACLE_CONFIG_FILE;
    System.out.println("Используется параметры доступа к ДБ oracle из " + ORACLE_CONFIG_FILE);

    if (argument(TEST) || argument(TEST_SHORT))
      NEED_TEST = true;

    if (argument(CLEAR_POSTGRES_DATA))
      NEED_CLEAR = true;

    String argRegex = argValue(REGEX_EX);
    TABLE_NAME_REGEX_TO_EXCLUDE = argRegex != null && !argRegex.isEmpty() ? argRegex : null;

    if (TABLE_NAME_REGEX_TO_EXCLUDE != null)
      System.out.println("При миграции будет использоваться исключающая маска для названий: " + TABLE_NAME_REGEX_TO_EXCLUDE);
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

  public static void prepareArguments(String[] args) {
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
    sb.append(String.format("\t %-16s - %s \n", MOCK_COUNT, "количество данных для тестов (по умолчанию - 1 000 000) доступно при включенном параметре тест"));
    sb.append(String.format("\t %-16s - %s \n", BATCH_SIZE, "размер батча записи (по умолчанию - 50 000)"));
    sb.append(String.format("\t %-16s - %s \n", POSTGRES_PROP, "полный путь к файлу с настройками доступа к БД postgres"));
    sb.append(String.format("\t %-16s - %s \n", ORACLE_PROP, "полный путь к файлу с настройками доступа к БД oracle"));
    sb.append(String.format("\t %-16s - %s \n", REGEX_EX, "регулярное выражение (маска) для исключения названий мигрируемых объектов"));
    sb.append(String.format("\t %-16s - %s \n", REGEX_EX_FILE, "файл содержащий регулярные выражения (маски) для исключения названий мигрируемых объектов"));
    sb.append(String.format("\t %-16s - %s \n", CLEAR_POSTGRES_DATA, "перед миграцией произвести чистку всех таблиц в назначении"));

    System.err.println(sb.toString());
    System.exit(0);
  }

}