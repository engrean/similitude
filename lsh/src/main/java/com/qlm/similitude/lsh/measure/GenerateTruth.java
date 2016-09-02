package com.qlm.similitude.lsh.measure;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class GenerateTruth {

  @SuppressWarnings("ResultOfMethodCallIgnored")
  public static void main(String[] args) {
    String sentencesFile = args[0];
    String outFile = args[1];
    Double minJaccardScore = Double.parseDouble(args[2]);
    try (BufferedReader br = new BufferedReader(new FileReader(sentencesFile))) {
      System.out.println("Reading sentences into cache");
      List<Set<String>> sentences = loadSentences(br);
      System.out.println("DONE: Reading sentences into cache");
      File outF = new File(outFile);
      outF.delete();
      try (BufferedWriter bw = new BufferedWriter(new FileWriter(outF))) {
        System.out.println("Scoring");
        for (int i = 0; i < sentences.size(); i++) {
          compareSentences(sentences.get(i), sentences, i+1, bw, minJaccardScore);
        }
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }

  }

  public static void compareSentences(Set<String> xSet, List<Set<String>> sentences, int start, Writer bw, double minJaccardScore) throws IOException {
    int xStart = start - 1;
    JaccardSimilarity score;
    for (int i = start; i < sentences.size(); i++) {
      score = new JaccardSimilarity(xSet, xStart, sentences.get(i), i);
      if (score.getScore() >= minJaccardScore) {
        bw.append(new JaccardSimilarity(xSet, xStart, sentences.get(i), i).toString()).append("\n");
      }
    }
  }

  public static List<Set<String>> loadSentences(BufferedReader reader) throws FileNotFoundException {
    List<Set<String>> sentences;
    if (reader != null) {
      sentences = reader.lines().map(line -> new HashSet<>(Arrays.asList(line.split(",")[1].split(" ")))).collect(Collectors.toList());
    } else {
      sentences = new ArrayList<>(0);
    }
    return sentences;
  }



}
