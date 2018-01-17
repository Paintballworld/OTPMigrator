package com.itechtopus;

import java.util.HashMap;
import java.util.Map;

public class Main {

  public static final String APP_DIR = "kaspiptp.d";
  public static String POSTGRES_CONFIG_FILE = System.getProperty("user.home") + "/" + APP_DIR + "/postgres.properties";
  public static String ORACLE_CONFIG_FILE = System.getProperty("user.home") + "/" + APP_DIR + "/oracle.properties";

  public static final String POSTGRES_PROP = "--postgres-prop";
  public static final String ORACLE_PROP = "--oracle-prop";
  public static final String MOCK_COUNT = "--mock-count";
  public static final String BATCH_SIZE = "--batch-size";
  public static int MAX_DATA_COUNT = 1_000_000;
  public static int MAX_BATCH_DATA = 50_000;
  public static final String HELP_SHORT = "-h";
  public static final String HELP = "--help";
  public static final String TEST = "--test";
  public static final String TEST_SHORT = "-t";

  private static Map<String, String> attributes = new HashMap<>();

  public static void main(String[] args) throws Exception {
    processAttributes(args);

    if (argument(HELP) || argument(HELP_SHORT))
      help();

    Integer argDataCount = intArg(MOCK_COUNT);
    MAX_DATA_COUNT = argDataCount != null ? argDataCount : MAX_DATA_COUNT;

    Integer argBatchSize = intArg(BATCH_SIZE);
    MAX_BATCH_DATA = argBatchSize != null ? argBatchSize : MAX_BATCH_DATA;

    if (argument(POSTGRES_PROP)) {
      String argPostgresFile = argValue(POSTGRES_PROP);
      POSTGRES_CONFIG_FILE = !argPostgresFile.isEmpty() ? argPostgresFile : POSTGRES_CONFIG_FILE;
    }
    System.out.println("Используется параметры доступа к ДБ postgres из " + POSTGRES_CONFIG_FILE);

    if (argument(ORACLE_PROP)) {
      String argPostgresFile = argValue(ORACLE_PROP);
      ORACLE_CONFIG_FILE = !argPostgresFile.isEmpty() ? argPostgresFile : ORACLE_CONFIG_FILE;
    }
    System.out.println("Используется параметры доступа к ДБ postgres из " + ORACLE_CONFIG_FILE);

    if (argument(TEST) || argument(TEST_SHORT))
      testOnly();




  }


  private static void testOnly() throws Exception {
    MigratorUtil.printTable();
    MigratorUtil.testMigration();
    System.exit(0);
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

  private static void processAttributes(String[] args) {
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

    System.err.println(sb.toString());
    System.exit(0);
  }




}
