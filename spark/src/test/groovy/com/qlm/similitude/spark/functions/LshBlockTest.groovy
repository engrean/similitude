package com.qlm.similitude.spark.functions

import spock.lang.Specification
import scala.Tuple2;


class LshBlockTest extends Specification {

  def 'generate lsh blocks in shifting the key'() {
    given:
    LshBlock lshBlock = new LshBlock(9, 7, true, false);
    when:
    List<Tuple2<String, Integer>> blocks = new ArrayList<>(lshBlock.call("0,one two three four five sixe seven eight nine ten eleven").collect())
    println blocks
    then:
    blocks.size() == 3
    blocks[0]._1().split('-').length == 7
    blocks[1]._1().split('-').length == 7
    blocks[2]._1().split('-').length == 7
  }

  def 'generate lsh blocks in traditional way'() {
    given:
    LshBlock lshBlock = new LshBlock(9, 3, false, false);
    when:
    List<Tuple2<String, Integer>> blocks = new ArrayList<>(lshBlock.call("0,one two three four").collect())
    then:
    blocks.size() == 3
    blocks[0]._1().split('-').length == 3
    blocks[1]._1().split('-').length == 3
    blocks[2]._1().split('-').length == 3
  }

}