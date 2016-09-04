package com.qlm.similitude.lsh.measure;

import java.text.DecimalFormat;

public class MatchCounts {
  private double score;
  private long found;
  private long truthNotFound;
  private long blockNotFound;
  private static final DecimalFormat precisionFormatter = new DecimalFormat("#.##");

  public MatchCounts(double score) {
    this.score = score;
  }

  public String pr() {
    double recall;
    if (found == 0) {
      recall = 0.0;
    } else {
      recall = convertToHundredths((double)found / ( (double)truthNotFound + (double)found));
    }
    return  score + "\t" + recall + "\t" + found +"/" + ( truthNotFound + found);
  }

  public double getScore() {
    return score;
  }

  public void setScore(double score) {
    this.score = score;
  }

  public long getFound() {
    return found;
  }

  public void setFound(long found) {
    this.found = found;
  }

  public long getTruthNotFound() {
    return truthNotFound;
  }

  public void setTruthNotFound(long truthNotFound) {
    this.truthNotFound = truthNotFound;
  }

  public long getBlockNotFound() {
    return blockNotFound;
  }

  public void setBlockNotFound(long blockNotFound) {
    this.blockNotFound = blockNotFound;
  }

  public Double convertToHundredths(Double dble) {
    return Double.valueOf(precisionFormatter.format(dble));
  }

}

