package com.qlm.similitude.spark;

import com.qlm.similitude.lsh.LshBlocking;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.text.DecimalFormat;
import java.util.*;

@SuppressWarnings("WeakerAccess") public class LshBlockAndEval {

  private static final DecimalFormat precisionFormatter = new DecimalFormat("#.##");

  public static void main(String[] args) {

    String sentencesIn = args[0];
    String truthIn = args[1];
    Integer numHashFuntions = Integer.parseInt(args[2]);
    Integer rowsPerBand = Integer.parseInt(args[3]);
    Boolean shiftKey = Boolean.parseBoolean(args[4]);
    Boolean compressKey = Boolean.parseBoolean(args[5]);
    JavaSparkContext sc = new JavaSparkContext();
    final JavaRDD<String> sentences = sc.textFile(sentencesIn);
    final JavaPairRDD<String, Set<Integer>> lshBlocks = sentences
      .flatMapToPair(new LshBlock(numHashFuntions, rowsPerBand, shiftKey, compressKey))
      .groupByKey()
      .mapToPair((PairFunction<Tuple2<String, Iterable<Integer>>, String, Set<Integer>>)blockKey->{
        Set<Integer> docIds = new HashSet<>();
        for (Integer docId: blockKey._2()) {
          docIds.add(docId);
        }
        return new Tuple2<>(blockKey._1(), docIds);
      }).repartition(sc.defaultParallelism()).cache();
    lshBlocks.saveAsTextFile("blocks");
    final JavaPairRDD<String, Double> lshPairs = lshBlocks
      .flatMapToPair(new GeneratePairs())
      .distinct();
//      .reduceByKey((Function2<Double, Double, Double>)(v1, v2)->v1);

//    lshPairs.saveAsTextFile("lshPairs");
    final Map<MatchRdd, Long> matches = sc
      .textFile(truthIn)
      .mapToPair((PairFunction<String, String, Double>)truth->{
        String[] parts = truth.split("\t");
        return new Tuple2<>(parts[0] + "|" + parts[1], Double.parseDouble(parts[2]));
      })
      .distinct()
      .union(lshPairs)
      .groupByKey()
      .map((Function<Tuple2<String, Iterable<Double>>, MatchRdd>)truthAndBlock->{
        boolean blockFound = false;
        boolean truthFound = false;
        Double score = -1.0;
        for (Double d: truthAndBlock._2()) {
          if (d == -1) {
            blockFound = true;
          } else if (d >= 0) {
            truthFound = true;
            score = d;
          }
        }
        return new MatchRdd(score, blockFound, truthFound);

      })
      .countByValue();

    printPRStats(matches);
    sc.stop();
  }

  protected static void printPRStats(Map<MatchRdd, Long> stats) {
    Map<Double, MatchCounts> scorePrs = new HashMap<>();
    MatchCounts mc;
    Long wastedCompares = 0L;
    for (MatchRdd key: stats.keySet()) {
      if (scorePrs.containsKey(key.jaccardScore)) {
        mc = scorePrs.get(key.jaccardScore);
      } else {
        mc = new MatchCounts(key.jaccardScore);
      }
      if (key.truthFound && key.blockFound) {
        mc.found = stats.get(key);
      } else if (key.truthFound) {
        mc.truthNotFound = stats.get(key);
      } else {
        mc.blockNotFound = stats.get(key);
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
    for (Double key: scores) {
      totalFound += scorePrs.get(key).found;
      totalNotFound += scorePrs.get(key).truthNotFound;
      if (scorePrs.get(key).score > -1.0) {
        System.out.println(scorePrs.get(key).pr());
      } else {
        wastedCompares = scorePrs.get(key).blockNotFound;
      }
    }
    System.out.println("Total Precision: " + ( (double)totalFound / ( totalFound + wastedCompares)));
    System.out.println("                 " + totalFound  + "/"+ totalFound +" + " + wastedCompares);
    System.out.println("Total Recall:    " + ((double)totalFound/(double)(totalNotFound+totalFound)));
    System.out.println("                 " + totalFound +"/"+ (totalNotFound+totalFound));
  }

  protected static Double convertToHundreths(Double dble) {
    return Double.valueOf(precisionFormatter.format(dble));
  }

  protected static class MatchCounts {
    private double score;
    private long found;
    private long truthNotFound;
    private long blockNotFound;

    protected MatchCounts(double score) {
      this.score = score;
    }

    protected String pr() {
      double recall;
      if (found == 0) {
        recall = 0.0;
      } else {
        recall = convertToHundreths((double)found/( (double)truthNotFound + (double)found));
      }
      return  score + "\t" + recall + "\t" + found +"/" + ( truthNotFound + found);
    }
  }

  protected static class LshBlock implements PairFlatMapFunction<String, String, Integer> {

    final private LshBlocking lshBlocking;

    protected LshBlock(int numHashFunctions, int rowsPerBand, boolean shiftKey, boolean compressKey) {
      lshBlocking = new LshBlocking(numHashFunctions, numHashFunctions/rowsPerBand, shiftKey, compressKey);
    }

    @Override public Iterator<Tuple2<String, Integer>> call(String sentence) throws Exception {
      String[] parts = sentence.split(",");
      Integer docId = Integer.parseInt(parts[0]);
      Set<String> lshKeys = lshBlocking.lsh(parts[1].split(" "));
      List<Tuple2<String, Integer>> blocks = new ArrayList<>();
      for (String block: lshKeys) {
        blocks.add(new Tuple2<>(block, docId));
      }
      return blocks.iterator();
    }
  }

  protected static class GeneratePairs implements PairFlatMapFunction<Tuple2<String, Set<Integer>>, String, Double> {
    @Override public Iterator<Tuple2<String, Double>> call(Tuple2<String, Set<Integer>> block) throws Exception {
      List<Tuple2<String, Double>> compares = new ArrayList<>();
      Integer[] docs = new Integer[block._2().size()];
      block._2().toArray(docs);
      Arrays.sort(docs);
      for (int i = 0; i < docs.length; i++) {
        for (int j = i + 1; j < docs.length; j++) {
          compares.add(new Tuple2<>(docs[i] + "|" + docs[j], -1.0));
        }
      }
      return compares.iterator();
    }
  }

  protected static class MatchRdd implements Serializable {
    final private Double jaccardScore;
    final private boolean blockFound;
    final private boolean truthFound;

    protected MatchRdd(Double jaccardScore, boolean blockFound, boolean truthFound) {
      this.jaccardScore = jaccardScore;
      this.blockFound = blockFound;
      this.truthFound = truthFound;
    }

    @Override
    public boolean equals(Object that) {
      boolean equal = false;
      if (that != null && that instanceof  MatchRdd) {
        MatchRdd tht = (MatchRdd)that;
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

}
