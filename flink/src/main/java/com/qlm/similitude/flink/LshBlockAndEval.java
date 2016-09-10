package com.qlm.similitude.flink;

import com.qlm.similitude.flink.function.GeneratePairs;
import com.qlm.similitude.flink.function.GroupSentenceIds;
import com.qlm.similitude.flink.function.LshBlock;
import com.qlm.similitude.lsh.measure.MatchPair;
import com.qlm.similitude.lsh.measure.Stats;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.qlm.similitude.lsh.measure.Stats.DEL;

public class LshBlockAndEval {

  public static void main(String[] args) throws Exception {
    final ParameterTool params = ParameterTool.fromArgs(args);
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().disableSysoutLogging();
    env.getConfig().setGlobalJobParameters(params);
    final String sentencesIn = params.get("sentences");
    final String truthIn = params.get("truth");
//    final Integer numHashFunctions = Integer.parseInt(params.get("h"));
    final Integer rowsPerBand = Integer.parseInt(params.get("r"));
//    final Boolean shiftKey = Boolean.parseBoolean(params.get("sk"));
    final Boolean compressKey = Boolean.parseBoolean(params.get("ck"));
    final Integer maxBlockSize = Integer.parseInt(params.get("mbs"));
    final String out = params.get("out");
    final Integer numRuns = Integer.parseInt(params.get("runs"));
    final Integer maxRowsPerBand = Integer.parseInt(params.get("mrb"));

    final DataSource<Tuple2<Integer, String>> sentences = getSentences(env, sentencesIn);

    final MapOperator<Tuple3<Integer, Integer, Double>, Tuple2<String, Double>> truth = getTruth(env, truthIn);

    List<Tuple2<String, Long>> scoreLines;
    int numHashes, numRows, numHashFuncsTemp;
    boolean printedHeader = false;
    File outFile = new File("lsh-"+System.currentTimeMillis()+".txt");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(outFile))) {
      for (boolean shift: new Boolean[]{false, true}) {
        for (int i = rowsPerBand; i <= maxRowsPerBand; i++){
          numRows = i;
          numHashFuncsTemp = numRows;
          for (int j = 1; j <= numRuns; j++) {
            numHashes = numHashFuncsTemp * j;
            scoreLines = runJob(env, numHashes, numRows, sentences, truth, shift, compressKey, maxBlockSize, out);

            final JobExecutionResult results = env.getLastJobExecutionResult();
            final TreeMap<Integer, Integer> blockCounts = results.getAccumulatorResult(GroupSentenceIds.BLOCK_SIZES);
            String header = "hashes" + DEL + "rows" + DEL + "mbs" + DEL + "lsh type" + DEL;
            String row = numHashes + DEL + numRows + DEL + maxBlockSize + DEL + ( shift ? "shift" : "trad" ) + DEL;
            String[] res = Stats.getPrStats(getMatchPairs(scoreLines));
            header += res[0];
            row += res[1];
            header += "blocks" + DEL + "oversize blocks" + DEL + "blocks of 1" + DEL;
            row += results.getAccumulatorResult(GroupSentenceIds.NUM_BLOCKS).toString();
            row += DEL + results.getAccumulatorResult(GroupSentenceIds.OVERSIZE_BLOCKS).toString();
            row += DEL + results.getAccumulatorResult(GroupSentenceIds.BLOCKS_OF_ONE).toString() + DEL;
            res = Stats.getBlockStats(blockCounts);
            header += res[0] + "\n";
            row += res[1];
            String output;
            if (printedHeader) {
              output = row;
            }
            else {
              output = "\n" + header + row;
              printedHeader = true;
            }
            System.out.println(output);
            bw.append(output);
          }
        }
      }

    }
  }

  private static List<Tuple2<String, Long>> runJob(ExecutionEnvironment env, Integer numHashFunctions, Integer rowsPerBand,
                                                   DataSource<Tuple2<Integer, String>> sentences,
                                                   MapOperator<Tuple3<Integer, Integer, Double>, Tuple2<String, Double>> truth, Boolean shiftKey,
                                                   Boolean compressKey, Integer maxBlockSize, String out) throws Exception {

    final DistinctOperator<Tuple2<String, Double>> sentencePairs = sentences
      .flatMap(new LshBlock(numHashFunctions, rowsPerBand, shiftKey, compressKey))
      .groupBy(0)
      .reduceGroup(new GroupSentenceIds(maxBlockSize))
      .filter((FilterFunction<Tuple2<String, List<Integer>>>)value->value.f1.size() > 1)
      .flatMap(new GeneratePairs())
      .distinct();
    sentences.writeAsText(out, FileSystem.WriteMode.OVERWRITE);
    env.execute();
    return truth
      .union(sentencePairs)
      .groupBy(0)
      .reduceGroup(new GroupReduceFunction<Tuple2<String, Double>, Tuple2<String, Long>>() {
        @Override public void reduce(Iterable<Tuple2<String, Double>> values, Collector<Tuple2<String, Long>> out1) throws Exception {
          boolean blockFound = false;
          boolean truthFound = false;
          Double score = -1.0;
          for (Tuple2<String, Double> value : values) {
            if (value.f1 == -1) {
              blockFound = true;
            }
            else if (value.f1 >= 0) {
              truthFound = true;
              score = value.f1;
            }
          }
          out1.collect(new Tuple2<>(score + "|" + blockFound + "|" + truthFound, 1L));
        }
      })
      .groupBy(0)
      .sum(1)
      .collect();
  }

  @SuppressWarnings("Convert2Lambda")
  private static MapOperator<Tuple3<Integer, Integer, Double>, Tuple2<String, Double>> getTruth(ExecutionEnvironment env, String truthIn) {
    return env.readCsvFile(truthIn).fieldDelimiter("\t")
              .includeFields(true, true, true, false)
              .types(Integer.class, Integer.class, Double.class)
              .map(
                new MapFunction<Tuple3<Integer, Integer, Double>, Tuple2<String,
                  Double>>() {
                  @Override public Tuple2<String, Double> map(
                    Tuple3<Integer, Integer, Double> value) throws Exception {
                    return new Tuple2<>(value.f0 + "|" + value.f1, value.f2);
                  }
                });
  }

  private static DataSource<Tuple2<Integer, String>> getSentences(ExecutionEnvironment env, String sentencesIn) {
    return env.readCsvFile(sentencesIn)
              .includeFields(true, true)
              .types(Integer.class, String.class);
  }

  private static Map<MatchPair, Long> getMatchPairs(List<Tuple2<String, Long>> matchLines) {
    Map<MatchPair, Long> matchPairs = new HashMap<>();
    for (Tuple2<String, Long> matchLine : matchLines) {
      matchPairs.put(new MatchPair(matchLine.f0), matchLine.f1);
    }
    return matchPairs;
  }


}
