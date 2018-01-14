package com.forsrc.spark.job;


import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.sql.SparkSession;

public final class HelloWorld {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder().appName("helloworld").getOrCreate();

        System.out.println("--> Hello world " + new Date());
        spark.stop();
    }
}