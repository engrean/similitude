package com.qlm.similitude.lsh.measure;

import java.io.Serializable;

public class MatchPair implements Serializable {
  final public Double jaccardScore;
  final public boolean blockFound;
  final public boolean truthFound;

  public MatchPair(String line) {
    String[] parts = line.split("\\|");
    jaccardScore = Double.parseDouble(parts[0]);
    blockFound = Boolean.parseBoolean(parts[1]);
    truthFound = Boolean.parseBoolean(parts[2]);
  }

  public MatchPair(Double jaccardScore, boolean blockFound, boolean truthFound) {
    this.jaccardScore = jaccardScore;
    this.blockFound = blockFound;
    this.truthFound = truthFound;
  }

  @Override
  public boolean equals(Object that) {
    boolean equal = false;
    if (that != null && that instanceof MatchPair) {
      MatchPair tht = (MatchPair)that;
      equal = tht.jaccardScore.equals(jaccardScore) && tht.blockFound == blockFound && tht.truthFound == truthFound;
    }
    return equal;
  }

  @Override
  public int hashCode() {
    return jaccardScore.hashCode() * Boolean.valueOf(blockFound).hashCode() * Boolean.valueOf(truthFound).hashCode();
  }

  @Override
  public String toString() {
    return jaccardScore + "|" + blockFound + "|" + truthFound;
  }
}