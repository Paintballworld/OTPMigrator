package kz.greetgo;

import static kz.greetgo.migration.MigrateWorker.TARGET_INSERT_QUERY;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ColumnUtilWorker implements AutoCloseable {

  private ResultSet resultSet;
  private ResultSetMetaData metaData;
  private int columnCount;
  private PreparedStatement statement;
  private String tableName;

  private Map<String, Integer> columnTypeMap = new HashMap<>();

  public ColumnUtilWorker(ResultSet resultSet, String tableName, Connection targetConnection) throws SQLException {
    this.resultSet = resultSet;
    this.tableName = tableName;
    init();
    statement = targetConnection.prepareStatement(buildQuery(TARGET_INSERT_QUERY));
  }

  public void addBatch() throws SQLException {
    for (int i = 1; i <= columnCount; i++) {
      setParameter(i);
    }
    statement.addBatch();
  }

  @Override
  public void close() throws Exception {
    statement.close();
  }

  public void executeBatch() throws SQLException {
    statement.executeBatch();
  }

  private String buildQuery(String initialQuery) throws SQLException {
    String insertSQL = initialQuery.replace("{{table_name}}", tableName);
    List<String> columnNames = new ArrayList<>();
    for (int i = 1; i <= columnCount; i++) {
      columnNames.add(metaData.getColumnName(i).toLowerCase().replaceAll("^right$", "right1"));
    }
    return insertSQL
      .replace("{{column_names}}", columnNames.stream().map(o -> "\"" + o + "\"").map(String::toLowerCase).collect(Collectors.joining(", ")))
      .replace("{{question_marks}}", columnNames.stream().map(o -> "?").collect(Collectors.joining(", ")));
  }

  private void init() throws SQLException {
    metaData = resultSet.getMetaData();
    columnCount = metaData.getColumnCount();
    for (int i = 1; i <= columnCount; i++) {
      columnTypeMap.put("" + i, metaData.getColumnType(i));
    }
  }

  private void setParameter(int columnIndex) throws SQLException {
    if (resultSet.getObject(columnIndex) == null) {
      statement.setNull(columnIndex, columnTypeMap.get("" + columnIndex));
      return;
    }
    switch (columnTypeMap.get("" + columnIndex)) {
      case 12: // VARCHAR2
        statement.setString(columnIndex, resultSet.getString(columnIndex));
        break;
      case 93: //TIMESTAMP
        statement.setTimestamp(columnIndex, resultSet.getTimestamp(columnIndex));
        break;
      case 2: //NUMBER
        statement.setLong(columnIndex, resultSet.getLong(columnIndex));
        break;
      case 2005: //CLOB
        statement.setClob(columnIndex, resultSet.getClob(columnIndex));
        break;
      default:
        throw new RuntimeException(String.format("Не указан сеттер:\n\t %-20s : %s\n\t %-20s : %s\n\t %-20s : %s",
          "Номер типа", "" + columnTypeMap.get("" + columnIndex),
          "Имя таблицы", "" + tableName ,
          "Индекс в запросе", "" + columnIndex));
    }
  }

}