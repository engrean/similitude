package com.qlm.similitude.flink;

import com.qlm.similitude.flink.function.GeneratePairs;
import com.qlm.similitude.flink.function.GroupSentenceIds;
import com.qlm.similitude.flink.function.LshBlock;
import com.qlm.similitude.lsh.measure.MatchPair;
import com.qlm.similitude.lsh.measure.PRStats;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DistinctOperator;
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

public class LshBlockAndEval {

  public static void main(String[] args) throws Exception {
    final ParameterTool params = ParameterTool.fromArgs(args);
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(params);
    final String sentencesIn = params.get("sentences");
    final String truthIn = params.get("truth");
    final Integer numHashFunctions = Integer.parseInt(params.get("h"));
    final Integer rowsPerBand = Integer.parseInt(params.get("r"));
    final Boolean shiftKey = Boolean.parseBoolean(params.get("sk"));
    final Boolean compressKey = Boolean.parseBoolean(params.get("ck"));
    final Integer maxBlockSize = Integer.parseInt(params.get("mbs"));
    final String out = params.get("out");
    final List<Tuple2<String, Long>> scoreLines = runJob(env, sentencesIn, truthIn, numHashFunctions, rowsPerBand, shiftKey, compressKey, maxBlockSize, out);

    File outFile = new File(numHashFunctions+"-"+rowsPerBand+"-"+shiftKey+"-"+compressKey+".txt");
    try(BufferedWriter bw = new BufferedWriter(new FileWriter(outFile))){
      String output = "\n# hashes: " + numHashFunctions + " rows per band: " + rowsPerBand + " shift key: " + shiftKey +"\n";
      output += PRStats.getPrStats(getMatchPairs(scoreLines));
      System.out.println(output);
      bw.append(output);
    }

  }

  @SuppressWarnings("Convert2Lambda")
  private static List<Tuple2<String, Long>> runJob(ExecutionEnvironment env, String sentencesIn, String truthIn, Integer numHashFunctions, Integer rowsPerBand,
                                                   Boolean shiftKey, Boolean compressKey, Integer maxBlockSize, String out) throws Exception {
    final CsvReader sentenceReader = env.readCsvFile(sentencesIn).includeFields(true, true);
    DataSource<Tuple2<Integer, String>> sentences = sentenceReader.types(Integer.class, String.class);
    final DistinctOperator<Tuple2<String, Double>> sentencePairs = sentences
      .flatMap(new LshBlock(numHashFunctions, rowsPerBand, shiftKey, compressKey))
      .groupBy(0)
      .reduceGroup(new GroupSentenceIds(maxBlockSize))
      .filter((FilterFunction<Tuple2<String, List<Integer>>>)value->value.f1.size() > 1)
      .flatMap(new GeneratePairs())
      .distinct();
    sentences.writeAsText(out, FileSystem.WriteMode.OVERWRITE);

    env.execute();
    final CsvReader truthReader = env.readCsvFile(truthIn).fieldDelimiter("\t").includeFields(true, true, true, false);
    return truthReader.types(Integer.class, Integer.class, Double.class)
      .map(new MapFunction<Tuple3<Integer,Integer,Double>, Tuple2<String, Double>>() {
        @Override public Tuple2<String, Double> map(Tuple3<Integer, Integer, Double> value) throws Exception {
          return new Tuple2<>(value.f0 + "|" + value.f1, value.f2);
        }
      })
      .distinct()
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

  private static Map<MatchPair, Long> getMatchPairs(List<Tuple2<String, Long>> matchLines) {
    Map<MatchPair, Long> matchPairs = new HashMap<>();
    for (Tuple2<String, Long> matchLine: matchLines) {
      matchPairs.put(new MatchPair(matchLine.f0), matchLine.f1);
    }
    return matchPairs;
  }


}
