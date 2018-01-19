package kz.greetgo.visualization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProgressBar {

  private static final int WIDTH = 15;

  private String tableName;
  private int total;
  private int current;
  private MigrationStatus status;
  private int animation = 1;
  private long startTime;
  private long elapsed;

  public ProgressBar() {
    resetWithNewTable("Undefined");
  }

  public void resetWithNewTable(String tableName) {
    this.tableName = tableName;
    this.total = 0;
    this.current = 0;
    this.status = MigrationStatus.RELEASED;
    this.elapsed = 0;
  }

  public void start(int total) {
    this.total = total;
    this.startTime = System.currentTimeMillis();
  }

  private String getStatusRepresentation() {
    switch (status) {
      case INITIALIZING: return "<init.>";
      case      READING: return "O>>B  p";
      case      WRITING: return "o  B>>P";
      case RELEASED: return "<close>";
      default          : return "< n/a >";
    }
  }

  public void setCurrent(int current) {
    this.current = current;
  }

  public boolean isFree() {
    return status == MigrationStatus.RELEASED;
  }

  public void setStatus(MigrationStatus status) {
    if (status == MigrationStatus.RELEASED)
      this.elapsed = System.currentTimeMillis() - startTime;
    this.status = status;
  }

  private String getStrRepresentationOfTime(long elapsed) {
    List<String> line = new ArrayList<>();
    LinkedHashMap<Integer, String> timeMap = new LinkedHashMap<>();
    timeMap.put(1000 * 60 * 60, " ч");
    timeMap.put(1000 * 60, " м");
    timeMap.put(1000, " c");
//    timeMap.put(1, " мc");
    for (Map.Entry<Integer, String> pair : timeMap.entrySet()) {
      if (elapsed > pair.getKey()) {
        int power = (int)(elapsed / pair.getKey());
        line.add(power + pair.getValue());
        elapsed %= pair.getKey();
      }
    }
    String elapsedStr = line.stream().collect(Collectors.joining(" "));
    if (elapsedStr.length() > WIDTH)
      elapsedStr = elapsedStr.substring(0, WIDTH - 3) + "...";
    return elapsedStr;
  }

  public String getProgressBar() {
    switch (status) {
      case INITIALIZING: return getInitProgressBar();
      case READING:
      case WRITING: return getWorkingProgressBar();
      case RELEASED: return getFinalizedProgressBar();
    }
    return "";
  }

  public String getTableName() {
    if (Arrays.asList(MigrationStatus.RELEASED, MigrationStatus.INITIALIZING).contains(status))
      return getFinalizedProgressBar();
    return getInitProgressBar();
  }

  private String getWorkingProgressBar() {
    if (status == MigrationStatus.RELEASED) return getFinalizedProgressBar();
    int percent = (int)(((float) current / (float) total) * (float)WIDTH) ;
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < WIDTH; i++) {
      sb.append(i <= percent ? "▓" : "░");
    }
    sb.append("]").append(getNextChar()).append(getStatusRepresentation());
    return sb.toString();
  }

  private String getInitProgressBar() {
    StringBuilder sb = new StringBuilder("[");
    sb.append(String.format("%." + WIDTH + "s",
      tableName.length() > WIDTH ?
        tableName.substring(0, 4) + "*" + tableName.substring(tableName.length() - WIDTH + 5) :
        tableName));
    for (int i = sb.length(); i <= WIDTH; i++) {
      sb.append(".");
    }
    sb.append("]").append(status == MigrationStatus.INITIALIZING ? "." : getNextChar()).append(getStatusRepresentation());
    return sb.toString();
  }

  private String getFinalizedProgressBar() {
    StringBuilder sb = new StringBuilder("[").append(getStrRepresentationOfTime(elapsed));
    for (int i = sb.length(); i <= WIDTH; i++) {
      sb.append(".");
    }
    sb.append("]").append("_").append(getStatusRepresentation());
    return sb.toString();
  }

  private String getNextChar() {
    animation = ++animation % 4;
    return "|/-\\".substring(animation, animation+1);
  }

  //
  // Main method
  //




  public static void main(String[] args) throws InterruptedException {
    ProgressBar pb = new ProgressBar();
    pb.resetWithNewTable("esb_forte_kazkom_registry_status");
    System.out.print("\r" + pb.getProgressBar());
    Thread.sleep(1000);
    pb.start(49);
    for (int i = 0; i < 49; i++) {
      pb.setCurrent(i);
      if (i % 4 == 0)
        pb.setStatus(MigrationStatus.READING);
      else
        pb.setStatus(MigrationStatus.WRITING);
      System.out.print("\r" + pb.getProgressBar());
      Thread.sleep(250);
    }
    pb.setStatus(MigrationStatus.RELEASED);
    System.out.print("\r" + pb.getProgressBar());
  }
}