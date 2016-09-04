package com.qlm.similitude.lsh.measure;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PRStats {

  public static String getPrStats(Map<MatchPair, Long> stats) {
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
    Arrays.sort(scores);
    long totalFound = 0, totalNotFound = 0;
    StringBuilder sb = new StringBuilder();
    for (Double key: scores) {
      totalFound += scorePrs.get(key).getFound();
      totalNotFound += scorePrs.get(key).getTruthNotFound();
      if (scorePrs.get(key).getScore() > -1.0) {
        sb.append(scorePrs.get(key).pr()).append("\n");
      } else {
        wastedCompares = scorePrs.get(key).getBlockNotFound();
      }
    }
    sb.append("Total Precision: ").append((double)totalFound / ( totalFound + wastedCompares )).append("\n");
    sb.append("                 ").append(totalFound).append("/").append(totalFound).append(" + ").append(wastedCompares).append("\n");
    sb.append("Total Recall:    ").append((double)totalFound / (double)( totalNotFound + totalFound )).append("\n");
    sb.append("                 ").append(totalFound).append("/").append(totalNotFound + totalFound).append("\n");
    return sb.toString();
  }

}
