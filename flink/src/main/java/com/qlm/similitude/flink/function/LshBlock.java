package com.qlm.similitude.flink.function;

import com.qlm.similitude.lsh.LshBlocking;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Set;

public class LshBlock implements FlatMapFunction<Tuple2<Integer, String>, Tuple2<String, Integer>> {

  final private LshBlocking lshBlocking;

  public LshBlock(int numHashFunctions, int rowsPerBand, boolean shiftKey, boolean compressKey) {
    lshBlocking = new com.qlm.similitude.lsh.LshBlocking(numHashFunctions, rowsPerBand, shiftKey, compressKey);
  }

  @Override public void flatMap(Tuple2<Integer, String> value, Collector<Tuple2<String, Integer>> out) throws Exception {
    Set<String> blocks = lshBlocking.lsh(value.f1.split(" "));
    for (String block: blocks) {
      out.collect(new Tuple2<>(block, value.f0));
    }
  }
}