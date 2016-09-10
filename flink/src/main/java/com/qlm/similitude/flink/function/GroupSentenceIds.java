package com.qlm.similitude.flink.function;

import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class GroupSentenceIds extends RichGroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, List<Integer>>> {
  private int maxBlockSize;
  public static final String OVERSIZE_BLOCKS = "oversize-blocks";
  public static final String NUM_BLOCKS = "num-blocks";
  public static final String BLOCK_SIZES = "blocks-sizes";
  public static final String BLOCKS_OF_ONE = "blocks-of-one";
  private final LongCounter numBlocks = new LongCounter();
  private final LongCounter blocksOfOne = new LongCounter();
  private final LongCounter namOversizeBlocks = new LongCounter();
  private final Histogram blockSizes = new Histogram();

  public GroupSentenceIds(int maxBlockSize) {
    this.maxBlockSize = maxBlockSize;
  }

  @Override
  public void open(final Configuration parameters) {
    getRuntimeContext().addAccumulator(OVERSIZE_BLOCKS, namOversizeBlocks);
    getRuntimeContext().addAccumulator(NUM_BLOCKS, numBlocks);
    getRuntimeContext().addAccumulator(BLOCKS_OF_ONE, blocksOfOne);
    getRuntimeContext().addAccumulator(BLOCK_SIZES, blockSizes);
  }

  @Override public void reduce(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, List<Integer>>> out) throws Exception {
    int count = 0;
    List<Integer> ids = new ArrayList<>();
    String key = null;
    numBlocks.add(1);
    boolean oversizedBlock = false;
    for (Tuple2<String, Integer> value : values) {
      if (count++ > maxBlockSize) {
        ids.clear();
        ids.add(-1);
        oversizedBlock = true;
        namOversizeBlocks.add(1);
        break;
      }
      else {
        ids.add(value.f1);
        key = value.f0;
      }
    }
    if (ids.size() > 1) {
      blockSizes.add(ids.size());
    } else if (ids.size() == 1 && !oversizedBlock) {
      blocksOfOne.add(1);
    } else {
      blockSizes.add(maxBlockSize+1);
    }
    out.collect(new Tuple2<>(key, ids));
  }
}
