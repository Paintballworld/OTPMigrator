package kz.greetgo.visualization;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProgressPool implements Runnable {

  private static final int DEFAULT_BAR_COUNT = 5;
  private static final int DISPLAYING_BARS_COUNT = 4;
  private static final int NAME_DISPLAY_COUNTER = 50;
  private boolean showTableName = false;

  private static AtomicBoolean stopFlag = new AtomicBoolean(false);
  private Map<String, ProgressBar> barPool = new HashMap<>();

  public ProgressPool(int initialProgressBarPool) {
    createBars(initialProgressBarPool);
  }

  public ProgressPool() {
    createBars();
  }

  public void createBars(int n) {
    for (int i = 0; i < n; i++) {
      barPool.put("" + i, new ProgressBar());
    }
  }

  public void createBars() {
    createBars(DEFAULT_BAR_COUNT);
  }

  public ProgressBar getBar(int order) {
    return barPool.get("" + order);
  }

  public ProgressBar getBar() {
    return getBar(0);
  }

  public synchronized void finish() {
    stopFlag.set(true);
  }

  public synchronized ProgressBar getFreeProgressBar() {
    while(true) {
      ProgressBar freeProgressBar = barPool.values().stream().filter(ProgressBar::isFree).findFirst().orElse(null);
      if (freeProgressBar != null) {
        freeProgressBar.setStatus(MigrationStatus.INITIALIZING);
        return freeProgressBar;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException ignore) {}
    }

  }

  @Override
  public void run() {
    int nameCountDown = NAME_DISPLAY_COUNTER;
    while (!stopFlag.get()) {
      if (--nameCountDown >= NAME_DISPLAY_COUNTER) {
        nameCountDown = NAME_DISPLAY_COUNTER / (showTableName ? 2 : 1);
        showTableName = !showTableName;
      }
      soutBars();
      try {
        Thread.sleep(220);
      } catch (InterruptedException ignore) {}
    }

    soutBars();
  }

  private void soutBars() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < Integer.min(barPool.size(), DISPLAYING_BARS_COUNT); i++)
      sb.append(i).append(":").append(showTableName ? getBar(i).getTableName() : getBar(i).getProgressBar()).append(" ::: ");

    if (barPool.size() > DISPLAYING_BARS_COUNT)
      sb.append("...");

    System.err.print("\r" + sb.toString());
  }
}