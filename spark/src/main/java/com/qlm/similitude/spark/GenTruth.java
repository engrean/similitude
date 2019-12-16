package com.qlm.similitude.spark;

import com.qlm.similitude.lsh.measure.GenerateTruth;
import com.qlm.similitude.lsh.measure.JaccardSimilarity;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.io.*;
import java.util.*;

public class GenTruth {

    public static void main(String[] args) {
        System.out.println("Inside GenTruth");
        final String sentencesIn = args[0];
        final String truthOut = args[1];
        final double minJaccardScore = Double.parseDouble(args[2]);
        final SparkSession spark = SparkSession.builder().appName("Generate Truth").config("header", true).getOrCreate();
        final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        List<Set<String>> sentences;
        try (BufferedReader br = new BufferedReader(new FileReader(sentencesIn))) {
            sentences = GenerateTruth.loadSentences(br);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw new NullPointerException(ioe.getMessage());
        }
        int numSentences = sentences.size();
        List<Integer> sentenceIds = new ArrayList<>(numSentences);
        for (int i = 0; i < numSentences; i++) {
            sentenceIds.add(i);
        }
        System.out.println("Found " + numSentences);
        Broadcast<List<Set<String>>> sentencesBv = sc.broadcast(sentences);
        sc.parallelize(sentenceIds)
                .flatMap(new GenTruthPairs(sentencesBv, minJaccardScore))
                .saveAsTextFile(truthOut);
    }

    protected final static class GenTruthPairs implements FlatMapFunction<Integer, String> {
        private final Broadcast<List<Set<String>>> sentences;
        private final double minJaccardScore;
        protected GenTruthPairs(Broadcast<List<Set<String>>> sentences, double minJaccardScore) {
            this.sentences = sentences;
            this.minJaccardScore = minJaccardScore;
        }

        @Override
        public Iterator<String> call(Integer startSentenceId) {
            Iterator<String> i = new GenTruthPairsIterator(sentences, startSentenceId, minJaccardScore);
            return i;
        }

    }

    protected final static class GenTruthPairsIterator implements Iterator<String> {
        private final Broadcast<List<Set<String>>> sentences;
        private final Set<String> sentenceX;
        private final int sentenceXId;
        private final double minJaccardScore;
        private final int numSentences;
        private int currentCompareId;
        private String next;

        protected GenTruthPairsIterator(final Broadcast<List<Set<String>>> sentences, int startSentenceId, double minJaccardScore){
            this.sentences = sentences;
            this.sentenceX = this.sentences.getValue().get(startSentenceId);
            this.sentenceXId = startSentenceId;
            this.minJaccardScore = minJaccardScore;
            this.currentCompareId = startSentenceId + 1;
            this.numSentences = sentences.getValue().size();
            prepareNext();
        }

        private void prepareNext() {
            next = null;
            Set<String> sentenceY;
            JaccardSimilarity score;
            while (currentCompareId < numSentences) {
                sentenceY = sentences.getValue().get(currentCompareId);
                score = new JaccardSimilarity(sentenceX, sentenceXId , sentenceY, currentCompareId++);
                if (score.getScore() >= minJaccardScore) {
                    next = score.toString();
                    break;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public String next() {
            String current = next;
            prepareNext();
            return current;
        }

    }
}
