package com.qlm.similitude.spark.functions;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

public class GeneratePairs implements PairFlatMapFunction<Tuple2<String, Set<Integer>>, String, Double> {

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

