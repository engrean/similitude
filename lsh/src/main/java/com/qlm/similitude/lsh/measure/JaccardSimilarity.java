package com.qlm.similitude.lsh.measure;

import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Set;

public class JaccardSimilarity {
  private String visualScore;
  private double score;
  private int xId;
  private int yId;
  private static final DecimalFormat scoreFormatter = new DecimalFormat("#.#");

  public JaccardSimilarity(Set<?> x, int xId, Set<?> y, int yId) {
    this.xId = xId;
    this.yId = yId;
    int found = 0, notFound = 0;
    Set<Object> newSet = new HashSet<>(x);
    newSet.addAll(y);
    for (Object o: newSet) {
      if (y.contains(o) && x.contains(o)) {
        found ++;
      } else {
        notFound ++;
      }
    }
    setScore(found, notFound);
  }

  public String getVisualScore() {
    return visualScore;
  }

  public double getScore() {
    return score;
  }

  public int getxId() {
    return xId;
  }

  public int getyId() {
    return yId;
  }

  @Override
  public String toString() {
    return xId + "\t" + yId + "\t" + scoreFormatter.format(score) + "\t" + visualScore;
  }

  private void setScore(int found, int notFound) {
    int denom = (found + notFound);
    if (found > 0 && denom > 0) {
      score = (double)found/(double)denom;
    } else {
      score = 0;
    }
    visualScore = found+"/"+denom;
  }

}
