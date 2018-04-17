package kz.greetgo.visualization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ProgressPool implements Runnable {

  private static final int DEFAULT_BAR_COUNT = 5;
  private static final int DISPLAYING_BARS_COUNT = 5;
  private static final int NAME_DISPLAY_COUNTER = 50;
  private boolean showTableName = false;

  private static AtomicBoolean stopFlag = new AtomicBoolean(false);
  private List<ProgressBar> barPool = new ArrayList<>();


  public ProgressPool() {
  }

  public synchronized void finish() {
    stopFlag.set(true);
  }

  public synchronized ProgressBar createBar() {
    ProgressBar bar = new ProgressBar();
    barPool.add(bar);
    return bar;
  }

  public synchronized ProgressBar createMainBar() {
    ProgressBar bar = new ProgressBar(30);
    barPool.add(bar);
    return bar;
  }

  @Override
  public void run() {
    int nameCountDown = NAME_DISPLAY_COUNTER;
    while (!stopFlag.get()) {
      if (--nameCountDown <= 0) {
        nameCountDown = NAME_DISPLAY_COUNTER / (showTableName ? 1 : 10);
        showTableName = !showTableName;
      }
      soutBars();
      try {
        Thread.sleep(220);
      } catch (InterruptedException ignore) {
        break;
      }
    }

    soutBars();
  }

  private void soutBars() {
    StringBuilder sb = new StringBuilder();
    final boolean showTableNames = showTableName;
    String progressString = barPool.stream()
      .limit(DISPLAYING_BARS_COUNT)
      .filter(Objects::nonNull)
      .map(o -> showTableNames ? o.getTableName() : o.getProgressBar())
      .collect(Collectors.joining(" ::: "));
    sb.append(progressString);

    if (barPool.size() >= DISPLAYING_BARS_COUNT)
      sb.append("...").append(barPool.size() - DISPLAYING_BARS_COUNT).append(" еще");

    System.err.print("\r" + sb.toString());
  }
}