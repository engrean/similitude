package com.qlm.similitude.lsh.measure;

import static com.qlm.similitude.lsh.measure.Stats.hundredths;

public class MatchCounts {
  private double score;
  private long found;
  private long truthNotFound;
  private long blockNotFound;

  public MatchCounts(double score) {
    this.score = score;
  }

  public String recall() {
    Double recall;
    if (found == 0) {
      recall = 0.0;
    } else {
      recall = hundredths((double)found / ( (double)truthNotFound + (double)found));
    }
    return recall.toString();
  }

  public double getScore() {
    return score;
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

}

