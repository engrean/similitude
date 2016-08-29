package com.qlm.similitude.lsh.measure;

import java.util.Arrays;

public class Main {

  public static void main(String[] args) {
    String command = args[0];
    if ("gen-sentences".equals(command)) {
      GenerateSentences.main(Arrays.copyOfRange(args, 1, args.length));
    } else if ("gen-truth".equals(command)) {
      GenerateTruth.main(Arrays.copyOfRange(args, 1, args.length));
    }
  }
}
