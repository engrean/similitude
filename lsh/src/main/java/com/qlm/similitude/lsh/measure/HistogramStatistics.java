package com.qlm.similitude.lsh.measure;

import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

public class HistogramStatistics
{
  public static Double average(final Map<Integer, Integer> histogram) {
    return HistogramStatistics.mean(histogram);
  }

  public static Double mean(final Map<Integer, Integer> histogram) {
    double sum = 0L;
    for (Integer value : histogram.keySet()) {
      sum += (value * histogram.get(value));
    }

    return sum / (double) HistogramStatistics.count(histogram);
  }

  public static Double percentile(final Map<Integer, Integer> histogram, final double percent) {
    if ((percent < 0d) || (percent > 1d)) {
      throw new IllegalArgumentException("Percentile must be between 0.00 and 1.00.");
    }

    if ((histogram == null) || histogram.isEmpty()) {
      return null;
    }

    double n = (percent * (HistogramStatistics.count(histogram).doubleValue() - 1d)) + 1d;
    double d = n - Math.floor(n);
    SortedSet<Integer> bins = new ConcurrentSkipListSet<>(histogram.keySet());
    long observationsBelowBinInclusive = 0L;
    Integer lowBin = bins.first();
    Double valuePercentile = null;

    for (Integer highBin : bins) {
      observationsBelowBinInclusive += histogram.get(highBin);

      if (n <= observationsBelowBinInclusive) {
        if ((d == 0f) || (histogram.get(highBin) > 1L)) {
          lowBin = highBin;
        }
        valuePercentile = lowBin.doubleValue() + ((highBin - lowBin) * d);
        break;
      }
      lowBin = highBin;
    }
    return valuePercentile;
  }

  public static Long count(final Map<Integer, Integer> histogram) {
    long observations = 0L;
    for (Integer value : histogram.keySet()) {
      observations += histogram.get(value);
    }

    return observations;
  }
}