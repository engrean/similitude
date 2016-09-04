package com.qlm.similitude.flink.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.List;

public class GeneratePairs implements FlatMapFunction<Tuple2<String,List<Integer>>, Tuple2<String, Double>> {
  @Override public void flatMap(Tuple2<String, List<Integer>> block, Collector<Tuple2<String, Double>> out) throws Exception {
    block.f1.sort((o1, o2)->( o1 < o2 ? -1 : ( o1 > o2 ? 1 : 0 ) ));
    for (int i = 0; i < block.f1.size(); i++) {
      for (int j = i + 1; j < block.f1.size(); j++) {
        out.collect(new Tuple2<>(block.f1.get(i) + "|" + block.f1.get(j), -1.0));
      }
    }
  }
}
