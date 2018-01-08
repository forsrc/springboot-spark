package com.forsrc.spark.web.wordcount.service.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.forsrc.spark.web.wordcount.service.WordCountService;

import scala.Tuple2;

@Service
public class WordCountServiceImpl implements WordCountService {

    @Autowired
    private transient JavaSparkContext sc;

    @Override
    public Map<String, Integer> wordCount(String filename) {
        JavaRDD<String> dataRdd = sc.textFile(filename);

        JavaRDD<String> wordsRdd = dataRdd.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });
        JavaPairRDD<String, Integer> wordCountPairRdd = wordsRdd.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2(word, 1);
            }
        });
        JavaPairRDD<String, Integer> countPairRdd = wordCountPairRdd
                .reduceByKey(new Function2<Integer, Integer, Integer>() {

                    @Override
                    public Integer call(Integer x, Integer y) throws Exception {
                        return x + y;
                    }
                });

        Map<String, Integer> map = new HashMap<>();
        countPairRdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {

            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t);
                map.put(t._1(), t._2());
            }
        });
        return map;
    }

}
