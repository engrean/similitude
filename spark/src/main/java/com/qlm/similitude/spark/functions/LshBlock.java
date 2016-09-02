package com.qlm.similitude.spark.functions;

import com.qlm.similitude.lsh.LshBlocking;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class LshBlock implements PairFlatMapFunction<String, String, Integer> {

  final private LshBlocking lshBlocking;

  public LshBlock(int numHashFunctions, int rowsPerBand, boolean shiftKey, boolean compressKey) {
    lshBlocking = new LshBlocking(numHashFunctions, rowsPerBand, shiftKey, compressKey);
  }

  @Override public Iterator<Tuple2<String, Integer>> call(String sentence) throws Exception {
    String[] parts = sentence.split(",");
    Integer docId = Integer.parseInt(parts[0]);
    Set<String> lshKeys = lshBlocking.lsh(parts[1].split(" "));
    return lshKeys.stream().map(block->new Tuple2<>(block, docId)).collect(Collectors.toList()).iterator();
  }
}
