package com.qlm.similitude.spark;

import com.qlm.similitude.lsh.LshBlockAsString;
import com.qlm.similitude.lsh.LshBlocking;
import com.qlm.similitude.lsh.LshBlocking64Bit;
import com.qlm.similitude.lsh.measure.MatchPair;
import com.qlm.similitude.lsh.measure.Stats;
import com.qlm.similitude.spark.functions.GeneratePairs;
import com.qlm.similitude.spark.functions.LshBlock;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuppressWarnings("WeakerAccess") public class LshBlockAndEval {


  public static void main(String[] args) {

    final String sentencesIn = args[0];
    final String truthIn = args[1];
    final int numHashFunctions = Integer.parseInt(args[2]);
    final int rowsPerBand = Integer.parseInt(args[3]);
    final boolean shiftKey = Boolean.parseBoolean(args[4]);
    final boolean compressKey = Boolean.parseBoolean(args[5]);
    final int maxBlockSize = Integer.parseInt(args[6]);
    final boolean use64Bit = Boolean.parseBoolean(args[7]);
    final String hashAlgorithm = args[8];
    final SparkSession spark = SparkSession.builder().appName("LSH Eval").config("header", true).getOrCreate();

    LshBlockAsString lshBlocker;
    if (use64Bit) {
      lshBlocker = new LshBlocking64Bit(numHashFunctions, rowsPerBand, shiftKey, compressKey, hashAlgorithm);
    } else {
      lshBlocker = new LshBlocking(numHashFunctions, rowsPerBand, shiftKey, compressKey, hashAlgorithm);
    }

    final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    final JavaRDD<String> sentences = sc.textFile(sentencesIn);
    final JavaPairRDD<String, List<Integer>> lshBlocks = sentences
      .flatMapToPair(new LshBlock(lshBlocker))
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
      })
      .filter((Function<Tuple2<String, List<Integer>>, Boolean>)block->block._2().size() > 1)
      .repartition(sc.defaultParallelism());

    final JavaPairRDD<String, Double> lshPairs = lshBlocks
      .flatMapToPair(new GeneratePairs())
      .distinct();

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
        double score = -1.0;
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

    String[] res = Stats.getPrStats(matches);
    System.out.println(res[0]);
    System.out.println(res[1]);
    sc.stop();
  }

}
