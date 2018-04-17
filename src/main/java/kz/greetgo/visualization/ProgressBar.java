package kz.greetgo.visualization;

import static kz.greetgo.MigratorUtil.getStrRepresentationOfTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;

public class ProgressBar {

  public static final Logger LOG = Logger.getLogger(ProgressBar.class);

  private int WIDTH = 20;

  private String tableName;
  private int total;
  private int current;
  private MigrationStatus status;
  private int animation = 1;
  private long startTime;
  private long elapsed;
  private boolean isMain = false;

  public ProgressBar() {
    resetWithNewTable("Undefined");
  }

  public ProgressBar(int width) {
    resetWithNewTable("Undefined");
    WIDTH = width;
    isMain = true;
  }

  public void release() {
    setStatus(MigrationStatus.RELEASED);
  }

  public void resetWithNewTable(String tableName) {
    this.tableName = tableName;
    this.total = 0;
    this.current = 0;
    this.status = MigrationStatus.RELEASED;
    this.elapsed = 0;
  }

  public void start(int total) {
    LOG.info("Начало для " + tableName + ", количество записей " + total);
    this.total = total;
    this.startTime = System.currentTimeMillis();
  }

  private String getStatusRepresentation() {
    switch (status) {
      case INITIALIZING: return "<init.>";
      case      READING: return "O>>B  p";
      case      WRITING: return "o  B>>P";
      case     RELEASED: return "<next.>";
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
    if (status == MigrationStatus.RELEASED) {
      final long l = System.currentTimeMillis() - startTime;
      this.elapsed = l;
      LOG.info("Конец для " + tableName + ", затрачено времени " + getStrRepresentationOfTime(l, 200));
    }
    this.status = status;
  }


  public String getProgressBar() {
    if (isMain) return getWorkingProgressBar();
    switch (status) {
      case INITIALIZING: return getInitProgressBar();
      case READING:
      case WRITING: return getWorkingProgressBar();
      case RELEASED: return getFinalizedProgressBar();
    }
    return "";
  }

  public String getTableName() {
    if (isMain) return getWorkingProgressBar();
    if (Arrays.asList(MigrationStatus.RELEASED, MigrationStatus.INITIALIZING).contains(status))
      return getFinalizedProgressBar();
    return getInitProgressBar();
  }

  private String getWorkingProgressBar() {
    if (status == MigrationStatus.RELEASED && !isMain) return getFinalizedProgressBar();
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
    StringBuilder sb = new StringBuilder("[").append(getStrRepresentationOfTime(elapsed, WIDTH));
    for (int i = sb.length(); i <= WIDTH; i++) {
      sb.append(".");
    }
    sb.append("]").append("_").append(getStatusRepresentation());
    if (sb.toString().replaceAll("\\.", "").startsWith("[]"))
      return getInitProgressBar();
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
      if (i % 7 == 0)
        pb.setStatus(MigrationStatus.READING);
      else
        pb.setStatus(MigrationStatus.WRITING);
      System.out.print("\r" + pb.getProgressBar());
      Thread.sleep(250);
    }
    pb.setStatus(MigrationStatus.RELEASED);
    System.out.print("\r" + pb.getProgressBar());
    System.out.println(getStrRepresentationOfTime(pb.elapsed, 200));
  }
}