package com.qlm.similitude.lsh.measure;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class GenerateSentences {

  public static void main(String[] args) {
    if (args.length == 6) {
      String topWordsFile = args[0];
      int numWords = Integer.parseInt(args[1]);
      int numSentences = Integer.parseInt(args[2]);
      int minWordsPerSentence = Integer.parseInt(args[3]);
      int maxWordsPerSentence = Integer.parseInt(args[4]);
      String out = args[5];
      try (BufferedReader br = new BufferedReader(new FileReader(topWordsFile))) {
        final List<String> topWords = loadWords(br, numWords);
        final Random randomWord = new Random(65432);
        final Random randomSentenceSize = new Random(32716);
        try (final BufferedWriter bw = new BufferedWriter(new FileWriter(out))) {
          generateAndWriteSentences(topWords, randomWord, randomSentenceSize, minWordsPerSentence, maxWordsPerSentence, numSentences, bw);
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    } else {
      for (String arg: args){
        System.out.print(arg);
        System.out.print(" ");
      }
      System.out.println("");
      System.out.println("Usage: <words file> <num_top_words> <num__sentences> <min_words> <max_words> <out file>");
    }
  }

  public static void generateAndWriteSentences(final List<String> topWords, final Random randomWord, final Random randomSentenceSize, final int minSentSize,
                                               final int maxSentSize, final int numSentences, final Writer writer) throws IOException {
    String prefix;
    for (int i = 0; i < numSentences; i++) {
      prefix = i+",";
      writer.append(prefix).append(generateSentence(topWords, randomWord, randomSentenceSize.nextInt(maxSentSize - minSentSize) + minSentSize));
      writer.append("\n");
    }
  }

  public static String generateSentence(List<String> words, Random randomWord, int numWords) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numWords; i++) {
      sb.append(words.get(randomWord.nextInt(words.size() - 1)));
      if ((i + 1) < numWords) {
        sb.append(" ");
      }
    }
    return sb.toString();
  }

  public static List<String> loadWords(BufferedReader reader, int numWords) throws FileNotFoundException {
    List<String> words;
    if (reader != null) {
      words = reader.lines().limit(numWords).map(line -> line.split("\t")[0]).collect(Collectors.toList());
    } else {
      words = new ArrayList<>(0);
    }
    return words;
  }

}
