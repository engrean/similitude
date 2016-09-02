package com.qlm.similitude.spark.functions;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

public class GeneratePairs implements PairFlatMapFunction<Tuple2<String, List<Integer>>, String, Double> {

  @Override public Iterator<Tuple2<String, Double>> call(Tuple2<String, List<Integer>> block) throws Exception {
    List<Tuple2<String, Double>> compares = new ArrayList<>();
    block._2().sort((o1, o2)->( o1 < o2 ? -1 : (o1 > o2 ? 1 : 0)));
    for (int i = 0; i < block._2().size(); i++) {
      for (int j = i + 1; j < block._2().size(); j++) {
        compares.add(new Tuple2<>(block._2().get(i) + "|" + block._2().get(j), -1.0));
      }
    }
    return compares.iterator();
  }

}

