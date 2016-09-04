package com.qlm.similitude.flink.function;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class GroupSentenceIds implements GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, List<Integer>>> {
  private int maxBlockSize;

  public GroupSentenceIds(int maxBlockSize) {
    this.maxBlockSize = maxBlockSize;
  }

  @Override public void reduce(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, List<Integer>>> out) throws Exception {
    int count = 0;
    List<Integer> ids = new ArrayList<>();
    String key = null;
    for (Tuple2<String, Integer> value : values) {
      System.out.println(value.f0);
      if (count++ > maxBlockSize) {
        ids.clear();
        ids.add(-1);
        break;
      }
      else {
        ids.add(value.f1);
        key = value.f0;
      }
    }

    out.collect(new Tuple2<>(key, ids));
  }
}
