package com.forsrc.spark.livy.job;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;

import static org.apache.spark.sql.functions.col;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class WordCountJob implements Job<Integer> {

    private static final Pattern SPACE = Pattern.compile(" ");

    private String filename;
    private String word;

    public WordCountJob(String filename, String word) {
        super();
        this.filename = filename;
        this.word = word;
    }

    @Override
    public Integer call(JobContext jobContext) throws Exception {

        SparkSession sparkSession = jobContext.sparkSession();

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
         .equalTo(this.word))
         .toJavaRDD()
         .collect()
         .forEach(i -> map.put(i.getString(0), (int)i.getLong(1)));

        Integer count = map.get(this.word);
        return count == null ? 0 : count.intValue();
    }


}
