package com.qlm.similitude.spark;

import com.qlm.similitude.spark.functions.GeneratePairs;
import com.qlm.similitude.spark.functions.LshBlock;
import com.qlm.similitude.spark.rdd.MatchPair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.text.DecimalFormat;
import java.util.*;

@SuppressWarnings("WeakerAccess") public class LshBlockAndEval {


  public static void main(String[] args) {

    final String sentencesIn = args[0];
    final String truthIn = args[1];
    final Integer numHashFunctions = Integer.parseInt(args[2]);
    final Integer rowsPerBand = Integer.parseInt(args[3]);
    final Boolean shiftKey = Boolean.parseBoolean(args[4]);
    final Boolean compressKey = Boolean.parseBoolean(args[5]);
    final Integer maxBlockSize = Integer.parseInt(args[6]);
    final JavaSparkContext sc = new JavaSparkContext();
    final JavaRDD<String> sentences = sc.textFile(sentencesIn);
    final JavaPairRDD<String, List<Integer>> lshBlocks = sentences
      .flatMapToPair(new LshBlock(numHashFunctions, rowsPerBand, shiftKey, compressKey))
      .groupByKey()
      .mapToPair((PairFunction<Tuple2<String, Iterable<Integer>>, String, List<Integer>>)blockKey->{
        List<Integer> docIds = new ArrayList<>();
        int count = 0;
        for (Integer docId: blockKey._2()) {
          if (count++ < maxBlockSize) {
            docIds.add(docId);
          } else {
            docIds.clear();
            docIds.add(-1);
            break;
          }
        }
        return new Tuple2<>(blockKey._1(), docIds);
      }).repartition(sc.defaultParallelism());

    final JavaPairRDD<String, Double> lshPairs = lshBlocks
      .flatMapToPair(new GeneratePairs())
      .distinct();
//      .reduceByKey((Function2<Double, Double, Double>)(v1, v2)->v1);

//    lshPairs.saveAsTextFile("lshPairs");
    final Map<MatchPair, Long> matches = sc
      .textFile(truthIn)
      .mapToPair((PairFunction<String, String, Double>)truth->{
        String[] parts = truth.split("\t");
        return new Tuple2<>(parts[0] + "|" + parts[1], Double.parseDouble(parts[2]));
      })
      .distinct()
      .union(lshPairs)
      .groupByKey()
      .map((Function<Tuple2<String, Iterable<Double>>, MatchPair>)truthAndBlock->{
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
        return new MatchPair(score, blockFound, truthFound);

      })
      .countByValue();

    printPRStats(matches);
    sc.stop();
  }

  protected static void printPRStats(Map<MatchPair, Long> stats) {
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

  protected static class MatchCounts {
    private double score;
    private long found;
    private long truthNotFound;
    private long blockNotFound;
    private static final DecimalFormat precisionFormatter = new DecimalFormat("#.##");

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

    protected static Double convertToHundreths(Double dble) {
      return Double.valueOf(precisionFormatter.format(dble));
    }

  }


}
