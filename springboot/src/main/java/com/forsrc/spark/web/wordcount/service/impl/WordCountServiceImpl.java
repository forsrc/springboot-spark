package com.forsrc.spark.web.wordcount.service.impl;

import static org.apache.spark.sql.functions.col;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.forsrc.spark.web.wordcount.service.WordCountService;

import scala.Tuple2;

@Service
public class WordCountServiceImpl implements WordCountService {

    @Value("${livy.url}")
    private String livyUrl;

    @Autowired
    private transient JavaSparkContext javaSparkContext;

    @Autowired
    private SparkSession sparkSession;

    private static final Pattern SPACE = Pattern.compile(" ");

    @Override
    public Map<String, Integer> wordCount(String filename) {
        return wordCount2(filename);
    }

    public Map<String, Integer> wordCount2(String filename) {
        final Map<String, Integer> map = new HashMap<>();

        Dataset<String> df = sparkSession.read()
                                         .text(filename)
                                         .as(Encoders.STRING());
        df.show();
        df.printSchema();

        Dataset<String> words = df.flatMap(word -> {
                                            System.out.println("--> " + word);
                                            return Arrays.asList(SPACE.split(word)).iterator();
                                        }, Encoders.STRING())
                                  .filter(word -> !word.isEmpty())
                                  .coalesce(1);


        words.printSchema();

        Dataset<Row> t = words.groupBy("value")
                              .count()
                              .toDF("word", "count");

        t = t.sort(functions.desc("count"));
 
        //t.toJavaRDD().saveAsTextFile("wordcount.out");

        t.toJavaRDD()
         .collect()
         .forEach(i -> map.put(i.getString(0), (int)i.getLong(1)));
        return map;
    }

    public Map<String, Integer> wordCount1(String filename) {

        Map<String, Integer> map = new HashMap<>();
        JavaRDD<String> lines = javaSparkContext.textFile(filename);
        JavaRDD<String> words = lines.flatMap(word -> Arrays.asList(SPACE.split(word)).iterator());
        JavaPairRDD<String, Integer> counts = words
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((Integer i1, Integer i2) -> (i1 + i2));
        List<Tuple2<String, Integer>> output = counts.collect();
        output.forEach(item -> map.put(item._1(), item._2()));


        return map;
    }

    @Override
    public int count(String filename, String str) {
        final Map<String, Integer> map = new HashMap<>();
 
        Dataset<String> df = sparkSession.read()
                                         .text(filename)
                                         .as(Encoders.STRING());
        df.show();
        df.printSchema();

        Dataset<String> words = df.flatMap(word -> Arrays.asList(SPACE.split(word)).iterator(), Encoders.STRING())
                                  .filter(word -> !word.isEmpty())
                                  .coalesce(1);


        words.printSchema();

        Dataset<Row> t = words.groupBy("value")
                              .count()
                              .toDF("word", "count");

        t = t.sort(functions.desc("count"));
 
        
        t.select("word", "count")
         .filter(col("word")
         .equalTo(str))
         .toJavaRDD()
         .collect()
         .forEach(i -> map.put(i.getString(0), (int)i.getLong(1)));
   
        // or
        t.createOrReplaceTempView("wordcount");
        Dataset<Row> sqlDF = sparkSession.sql(String.format("SELECT * FROM wordcount WHERE word = \"%s\"", str));

        sqlDF.show();
        sqlDF.toJavaRDD()
             .collect()
             .forEach(i -> map.put(i.getString(0), (int)i.getLong(1)));

        Integer count = map.get(str);
        return count == null ? 0 : count.intValue();
    }

}
