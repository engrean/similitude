package com.qlm.similitude.lsh.measure

import spock.lang.Specification
import static GenerateSentences.*


class GenerateSentencesTest extends Specification {

  GenerateSentences generateSentences = new GenerateSentences()

  def 'generateAndWriteSentences'() {
    given:
    StringWriter sw = new StringWriter();
    Random randomWord = new Random(2)
    Random randomSentenceSize = new Random(4)

    when:
    List<String> words = loadWords(stringToBufferedReader(wordContent), 20)
    generateAndWriteSentences(words, randomWord, randomSentenceSize, 5, 20, 5, sw)
    def sentences = sw.toString().split("[\\n\\r]+")

    then:
    sentences.length == 5
    between(sentences[0], 5, 20)
    between(sentences[1], 5, 20)
    between(sentences[2], 5, 20)
    between(sentences[3], 5, 20)
    between(sentences[4], 5, 20)
  }

  private static boolean between(String sentence, int min, int max) {
    String[] words = sentence.split(' ')
    return words.length >= min && words.length <= max
  }

  def 'generateSentence'() {
    when:
    Random random = new Random(2342)
    String sentence = generateSentence(loadWords(stringToBufferedReader(wordContent), 3), random, 3)
    String[] words = sentence.split(" ")
    Set<String> uniqueWords = new HashSet<>(Arrays.asList(words))

    then:
    words.length == 3
    uniqueWords.size() > 1
  }

  def 'loadWords: with null as reader'() {
    when:
    String[] words = loadWords(null, 100)

    then:
    words.size() == 0
  }

  def 'loadWords: loading in a only 3 words when 5 were asked for'() {
    when:
    String contents = """one\t1234
two\t34234
three\t12
"""
    def words = loadWords(stringToBufferedReader(contents), 5)

    then:
    words.size() == 3
    words[0] == 'one'
    words[1] == 'two'
    words[2] == 'three'
  }

  def 'loading in a only 2 words when 3 were available'() {
    when:
    String contents = """one\t1234
two\t34234
three\t12
"""
    def words = loadWords(stringToBufferedReader(contents), 2)

    then:
    words.size() == 2
    words[0] == 'one'
    words[1] == 'two'
  }

  private static BufferedReader stringToBufferedReader(String contents) {
    return new BufferedReader(new StringReader(contents))
  }

  String wordContent = """one\t1
two\t2
three\t3
four\t4
five\t5
six\t5
seven\t5
eight\t5
nine\t5
ten\t5
eleven\t5
"""

}