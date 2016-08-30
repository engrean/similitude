package com.qlm.similitude.lsh.measure

import spock.lang.Specification

import static com.qlm.similitude.lsh.measure.GenerateTruth.loadSentences

class GenerateTruthTest extends Specification {

  def 'compareSentences: four values one with score below 0.1'() {
    given:
    List<Set<String>> sentences = []
    sentences << new HashSet<>(["one", "two", "three"])
    sentences << new HashSet<>(["two", "three", "four"])
    sentences << new HashSet<>(["three", "four", "five"])
    sentences << new HashSet<>(["four", "five", "six"])
    StringWriter writer = new StringWriter()

    when:
    GenerateTruth.compareSentences(sentences[0], sentences, 1, writer)

    then:
    writer.toString() == """0\t1\t0.5\t2/4
0\t2\t0.2\t1/5
"""
  }

  def 'compareSentences: three values'() {
    given:
    List<Set<String>> sentences = []
    sentences << new HashSet<>(["one", "two", "three"])
    sentences << new HashSet<>(["two", "three", "four"])
    sentences << new HashSet<>(["three", "four", "five"])
    StringWriter writer = new StringWriter()

    when:
    GenerateTruth.compareSentences(sentences[0], sentences, 1, writer)

    then:
    writer.toString() == """0\t1\t0.5\t2/4
0\t2\t0.2\t1/5
"""
  }

  def 'compareSentences: two values'() {
    given:
    List<Set<String>> sentences = []
    sentences << new HashSet<>(["one", "two", "three"])
    sentences << new HashSet<>(["two", "three", "four"])
    StringWriter writer = new StringWriter()

    when:
    GenerateTruth.compareSentences(sentences[0], sentences, 1, writer)

    then:
    writer.toString() == """0\t1\t0.5\t2/4
"""
  }

  def 'compareSentences: only one value'() {
    given:
    List<Set<String>> sentences = []
    sentences << new HashSet<>(["one", "two", "three"])
    StringWriter writer = new StringWriter()

    when:
    GenerateTruth.compareSentences(sentences[0], sentences, 1, writer)

    then:
    !writer.toString()
  }

  def 'loadSentences: with null as reader'() {
    when:
    String[] words = loadSentences(null)

    then:
    words.size() == 0
  }

  def 'loadSentences: loads in all sentences'() {
    given:
    String contents = """0,one two three
1,four five six
2,three five one
"""
    when:
    def words = loadSentences(stringToBufferedReader(contents))

    then:
    words.size() == 3
    words[0] == ['one', 'two', 'three'] as Set<String>
    words[1] == ['four', 'five', 'six'] as Set<String>
    words[2] == ['three', 'five', 'one'] as Set<String>
  }

  private static BufferedReader stringToBufferedReader(String contents) {
    return new BufferedReader(new StringReader(contents))
  }

}