package com.qlm.similitude.lsh.measure

import spock.lang.Specification


/**
 * Created by hargravescw on 8/27/16.*/
class JaccardSimilarityTest extends Specification {

  JaccardSimilarity similarity;

  def "[1,2,3] vs [4,5,6]" () {
    given:
    similarity = new JaccardSimilarity([1,2,3] as Set, 0, [4,5,6] as Set, 1)

    expect:
    similarity.score == 0
    similarity.visualScore == '0/6'
  }

  def "toString for [1,2,3] vs [4,5,6]" () {
    given:
    similarity = new JaccardSimilarity([1,2,3] as Set, 0, [4,5,6] as Set, 1)

    expect:
    similarity.toString() == '0\t1\t0\t0/6'
  }

  def "[1,2,3] vs [4,5]" () {
    given:
    similarity = new JaccardSimilarity([1,2,3] as Set, 0, [4,5] as Set, 1)

    expect:
    similarity.score == 0
    similarity.visualScore == '0/5'
  }

  def "[1,2,3] vs [3]" () {
    given:
    similarity = new JaccardSimilarity([1,2,3] as Set, 0, [3] as Set, 1)

    expect:
    similarity.score == (double)1/(double)3
    similarity.visualScore == '1/3'
  }

}