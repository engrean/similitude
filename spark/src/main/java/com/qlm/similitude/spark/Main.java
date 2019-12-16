package com.qlm.similitude.spark;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {
        if (args.length > 0){
            String[] params = Arrays.copyOfRange(args, 1, args.length);
            if (args[0].equalsIgnoreCase("blockAndEval")) {
                LshBlockAndEval.main(params);
            } else if (args[0].equalsIgnoreCase("genTruth")) {
                GenTruth.main(params);
            } else {
                System.out.println(args[0] + " is not a known Spark Job");
            }
        }
    }
}
