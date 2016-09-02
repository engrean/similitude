package com.qlm.similitude.spark.functions

import scala.Tuple2
import spock.lang.Specification

class GeneratePairsTest extends Specification {
  GeneratePairs generatePairs = new GeneratePairs()

  def 'generatePairs in order' () {
    given:
    String key = '1'
    List<Integer> docs = [200, 23, 1, 3000]
    Tuple2<String, List<Integer>> blocks = new Tuple2<>(key, docs)

    when:
    def iterator = generatePairs.call(blocks)

    then:
    iterator.next()._1() == '1|23'
    iterator.next()._1() == '1|200'
    iterator.next()._1() == '1|3000'
    iterator.next()._1() == '23|200'
    iterator.next()._1() == '23|3000'
    iterator.next()._1() == '200|3000'
    !iterator.hasNext()
  }

}