package com.qlm.similitude.lsh.measure;

import java.text.DecimalFormat;
import java.util.*;

public class Stats {

  public static final String DEL = "\t";
  private static final DecimalFormat tenthsFormatter = new DecimalFormat("#.#");
  private static final DecimalFormat hundredthsFormatter = new DecimalFormat("#.##");
  private static final DecimalFormat thousandsPrecisionFormatter = new DecimalFormat("#.###");

  public static String[] getPrStats(Map<MatchPair, Long> stats) {
    Map<Double, MatchCounts> scorePrs = new HashMap<>();
    MatchCounts mc;
    Long wastedCompares = 0L;
    for (MatchPair key: stats.keySet()) {
      if (scorePrs.containsKey(key.jaccardScore)) {
        mc = scorePrs.get(key.jaccardScore);
      } else {
        mc = new MatchCounts(key.jaccardScore);
      }
      if (key.truthFound && key.blockFound) {
        mc.setFound(stats.get(key));
      } else if (key.truthFound) {
        mc.setTruthNotFound(stats.get(key));
      } else {
        mc.setBlockNotFound(stats.get(key));
      }
      scorePrs.put(key.jaccardScore, mc);
    }
    Double[] scores = new Double[scorePrs.size()];
    int count = 0;
    for(Double scr: scorePrs.keySet()) {
      scores[count++] = scr;
    }
    Arrays.sort(scores, (o1, o2)->( o1 > o2 ? -1 : ( Objects.equals(o1, o2) ? 0 : 1)));
    long totalFound = 0, totalNotFound = 0;

    for (Double key: scores) {
      totalFound += scorePrs.get(key).getFound();
      totalNotFound += scorePrs.get(key).getTruthNotFound();
      if (scorePrs.get(key).getScore() == -1.0) {
        wastedCompares = scorePrs.get(key).getBlockNotFound();
      }
    }
    StringBuilder sb = new StringBuilder();
    StringBuilder header = new StringBuilder();
    double precision = thousandths((double)totalFound / ( totalFound + wastedCompares ));
    double recall = thousandths((double)totalFound / (double)( totalNotFound + totalFound ));
    header.append("precision").append(DEL).append("recall").append(DEL).append("recall nums").append(DEL);
    for (Double key: scores) {
      if (scorePrs.get(key).getScore() > -1.0) {
        header.append(key).append(" recall").append(DEL).append(key).append(" recall nums").append(DEL);
      }
    }
    sb.append(precision).append(DEL).append(recall).append(DEL).append(totalFound).append("/").append((totalNotFound+totalFound)).append(DEL);
    for (Double key: scores) {
      mc = scorePrs.get(key);
      if (mc.getScore() > -1.0) {
        sb.append(mc.recall()).append(DEL).append(mc.getFound()).append(DEL).append(mc.getFound()).append("/").append(mc.getFound()+mc.getTruthNotFound()).append(DEL);
      }
    }
    //Returning an array with header and output is hacky
    return new String[]{header.toString(), sb.toString()};
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public static String[] getBlockStats(Map<Integer, Integer> blockCounts) {
    Double avg = tenths(HistogramStatistics.average(blockCounts));
    Double ten = tenths(HistogramStatistics.percentile(blockCounts, 0.10));
    Double twentyfive = tenths(HistogramStatistics.percentile(blockCounts, 0.25));
    Double fifty = tenths(HistogramStatistics.percentile(blockCounts, 0.50));
    Double seventyfive = tenths(HistogramStatistics.percentile(blockCounts, 0.75));
    Double ninetynine = tenths(HistogramStatistics.percentile(blockCounts, 0.99));
    Double threenines = tenths(HistogramStatistics.percentile(blockCounts, 0.999));
    Integer max = blockCounts.keySet().stream().max((o1, o2)->( o1 > o2 ? 1 : ( o1 < o2 ? -1 : 0))).get();
    String header = "10th" + DEL + "25th" + DEL + "50th" + DEL + "75th" + DEL + "99th" + DEL + "99.9th" + DEL + "max" + DEL + "average";
    String data = ten + DEL + twentyfive + DEL + fifty + DEL + seventyfive + DEL + ninetynine + DEL + threenines + DEL + max + DEL + avg;
    //Returning an array with header and output is hacky
    return new String[]{header, data};
  }

  public static Double tenths(Double dble) {
    return Double.valueOf(tenthsFormatter.format(dble));
  }

  public static Double hundredths(Double dble) { return Double.valueOf(hundredthsFormatter.format(dble)); }

  public static Double thousandths(Double dble) { return Double.valueOf(thousandsPrecisionFormatter.format(dble)); }


}
