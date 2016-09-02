package com.qlm.similitude.lsh

import spock.lang.Specification


class LshBlockingTest extends Specification {

  LshBlocking lshBlocking = new LshBlocking(12, 2, false, false)

  def 'bandsToString null'() {
    when:
    def hash = lshBlocking.bandsToStrings(null)

    then:
    hash.size() == 0
  }

  def 'bandsToString empty list'() {
    when:
    def hash = lshBlocking.bandsToStrings([] as int[][])

    then:
    hash.size() == 0
  }

  def 'bandToString null'() {
    when:
    def hash = lshBlocking.bandToString(null)

    then:
    hash == ''
  }

  def 'bandToString empty list'() {
    when:
    def hash = lshBlocking.bandToString([] as int[])

    then:
    hash == ''
  }

  def 'bandToString'() {
    given:
    lshBlocking = new LshBlocking(12, 2, false, true)
    when:
    def hash = lshBlocking.bandToString([v1, v2, v3, v4] as int[])

    then:
    hash == expected

    where:
    expected  | v1 | v2 | v3 | v4
    'GJQrdBBfioZbQr0V2dKCiA' | 0  | 0  | 0  | 0
    'sAi6wxULIhmRZAN2ljYEAg' | 9  | 9  | 9  | 9
    '1qY4WDoQYuMeROGpWVuHlA' | 10 | 0  | 1  | 11
  }

  def 'lsh 3 hashes 2 rows shiftKey'() {
    def lsh = new LshBlocking(3, 2, true, true)

    when:
    int[][] minHash = lsh.lsh([2, 3, 4, 5, 1, 2, 3] as int[])

    then:
    minHash.length == 2
    minHash[0].length == 2
    minHash[0][0] == 1
    minHash[0][1] == 3
    minHash[1].length == 2
    minHash[1][0] == 3
    minHash[1][1] == 3
  }

  def 'lsh 9 hashes 7 rows shiftKey'() {
    def lsh = new LshBlocking(9, 7, true, false)

    when:
    int[] minHash = lsh.minHash([2, 3, 4, 5, 1, 2, 3] as int[])
    def mhBlocks = lsh.bandToString(minHash)
    int[][] lshBlocks = lsh.lsh([2, 3, 4, 5, 1, 2, 3] as int[])
    def bands = lsh.bandsToStrings(lshBlocks).asList()
    println mhBlocks
    println bands

    then:
    bands.size() == 3
    bands[0].split('-').length == 7
    bands[1].split('-').length == 7
    bands[2].split('-').length == 7
  }

  def 'lsh 2 hashes 1 band'() {
    def lsh = new LshBlocking(2, 1)

    when:
    int[][] minHash = lsh.lsh([2, 3, 4, 5, 1, 2, 3] as int[])

    then:
    minHash.length == 1
    minHash[0].length == 2
    minHash[0][0] == 1
    minHash[0][1] == 3
  }

  def 'lsh 2 hashes 2 band'() {
    def lsh = new LshBlocking(2, 2)

    when:
    int[][] minHash = lsh.lsh([2, 3, 4, 5, 1, 2, 3] as int[])

    then:
    minHash.length == 2
    minHash[0].length == 1
    minHash[0][0] == 1
    minHash[1][0] == 3
  }

  def 'minHash 4 hashes'() {
    def lsh = new LshBlocking(4, 1)

    when:
    int[] minHash = lsh.minHash([2, 3, 4, 5, 1, 2, 3] as int[])

    then:
    minHash.length == 4
    minHash[0] == 1
    minHash[1] == 3
    minHash[2] == 3
    minHash[3] == 1
  }

  def 'minHash 2 hashes'() {
    def lsh = new LshBlocking(2, 1)

    when:
    int[] minHash = lsh.minHash([2, 3, 4, 5, 1, 2, 3] as int[])

    then:
    minHash.length == 2
    minHash[0] == 1
    minHash[1] == 3
  }

  def 'minHashN with lots a values the min being towards the end'() {
    def lsh = new LshBlocking(2, 1)

    when:
    int min = lsh.minHashN([2, 3, 4, 5, 1, 2, 3] as int[], 0)

    then:
    min == 1
  }

  def 'minHashN with two values'() {
    def lsh = new LshBlocking(2, 1)

    when:
    int min = lsh.minHashN([1, 2] as int[], 0)

    then:
    min == 1
  }

  def 'getHash with 0'() {
    def lsh = new LshBlocking(2, 1)

    expect:
    lsh.getHash(1, 0) == 1
  }

  def 'getHash with 1'() {
    def lsh = new LshBlocking(2, 1)

    expect:
    lsh.getHash(1, 0) == 1
  }

  def 'hashOhobValues'() {
    given:
    def lsh = new LshBlocking(2, 1)

    when:
    int[] valuesHashes = lsh.hashValues('1', '2', '3')

    then:
    valuesHashes.length == 3
    valuesHashes[0] == -1810453357
    valuesHashes[1] == 19522071
    valuesHashes[2] == 264741300
  }

  def "constructor with divisible hash functions and bands"() {
    when:
    new LshBlocking(numHashes, numBands)

    then:
    notThrown(IllegalArgumentException)

    where:
    numHashes | numBands
    2         | 1
    4         | 2
    64        | 8
  }

  def "constructor with indivisible hash functions and bands"() {
    when:
    new LshBlocking(numHashes, numBands)

    then:
    def ex = thrown(IllegalArgumentException)
    ex.message == 'The the number of hash functions must be evenly divisible by the number of bands'

    where:
    numHashes | numBands
    4         | 3
    3         | 4
  }

}