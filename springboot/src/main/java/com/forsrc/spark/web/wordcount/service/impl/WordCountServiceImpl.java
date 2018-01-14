package com.forsrc.spark.web.wordcount.service.impl;

import static org.apache.spark.sql.functions.col;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.hadoop.yarn.webapp.example.HelloWorld;
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

import com.bluebreezecf.tools.sparkjobserver.api.ISparkJobServerClient;
import com.bluebreezecf.tools.sparkjobserver.api.ISparkJobServerClientConstants;
import com.bluebreezecf.tools.sparkjobserver.api.SparkJobConfig;
import com.bluebreezecf.tools.sparkjobserver.api.SparkJobInfo;
import com.bluebreezecf.tools.sparkjobserver.api.SparkJobJarInfo;
import com.bluebreezecf.tools.sparkjobserver.api.SparkJobResult;
import com.bluebreezecf.tools.sparkjobserver.api.SparkJobServerClientException;
import com.bluebreezecf.tools.sparkjobserver.api.SparkJobServerClientFactory;
import com.cloudera.livy.JobHandle;
import com.cloudera.livy.LivyClient;
import com.cloudera.livy.LivyClientBuilder;
import com.forsrc.spark.livy.job.HelloWorldJob;
import com.forsrc.spark.livy.job.WordCountJob;
import com.forsrc.spark.utils.JarFileUtils;
import com.forsrc.spark.web.wordcount.service.WordCountService;

import scala.Tuple2;

@Service
public class WordCountServiceImpl implements WordCountService {

    @Value("${livy.url}")
    private String livyUrl;
    @Value("${jobserver.url}")
    private String jobserverUrl;

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

        Dataset<String> df = sparkSession.read().text(filename).as(Encoders.STRING());
        df.show();
        df.printSchema();

        Dataset<String> words = df.flatMap(word -> {
            System.out.println("--> " + word);
            return Arrays.asList(SPACE.split(word)).iterator();
        }, Encoders.STRING()).filter(word -> !word.isEmpty()).coalesce(1);

        words.printSchema();

        Dataset<Row> t = words.groupBy("value").count().toDF("word", "count");

        t = t.sort(functions.desc("count"));

        t.toJavaRDD().saveAsTextFile("wordcount.out");

        t.toJavaRDD().collect().forEach(i -> map.put(i.getString(0), (int) i.getLong(1)));
        return map;
    }

    public Map<String, Integer> wordCount1(String filename) {

        Map<String, Integer> map = new HashMap<>();
        JavaRDD<String> lines = javaSparkContext.textFile(filename);
        JavaRDD<String> words = lines.flatMap(word -> Arrays.asList(SPACE.split(word)).iterator());
        JavaPairRDD<String, Integer> counts = words.mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((Integer i1, Integer i2) -> (i1 + i2));
        List<Tuple2<String, Integer>> output = counts.collect();
        output.forEach(item -> map.put(item._1(), item._2()));

        return map;
    }

    @Override
    public int count(String filename, String str) {
        final Map<String, Integer> map = new HashMap<>();

        Dataset<String> df = sparkSession.read().text(filename).as(Encoders.STRING());
        df.show();
        df.printSchema();

        Dataset<String> words = df.flatMap(word -> Arrays.asList(SPACE.split(word)).iterator(), Encoders.STRING())
                .filter(word -> !word.isEmpty()).coalesce(1);

        words.printSchema();

        Dataset<Row> t = words.groupBy("value").count().toDF("word", "count");

        t = t.sort(functions.desc("count"));

        t.select("word", "count").filter(col("word").equalTo(str)).toJavaRDD().collect()
                .forEach(i -> map.put(i.getString(0), (int) i.getLong(1)));

        // or
        t.createOrReplaceTempView("wordcount");
        Dataset<Row> sqlDF = sparkSession.sql(String.format("SELECT * FROM wordcount WHERE word = \"%s\"", str));

        sqlDF.show();
        sqlDF.toJavaRDD().collect().forEach(i -> map.put(i.getString(0), (int) i.getLong(1)));

        Integer count = map.get(str);
        return count == null ? 0 : count.intValue();
    }

    @Override
    public int livyCount(String filename, String word) {

        try {
            return JarFileUtils.handle(livyUrl, WordCountJob.class, new WordCountJob(filename, word));
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

    }

    @Override
    public String livyHelloworld() {
        try {
            return JarFileUtils.handle(livyUrl, HelloWorldJob.class, new HelloWorldJob());
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @Override
    public String jobserverHelloworld() {

        ISparkJobServerClient client = null;
        try {
            client = SparkJobServerClientFactory.getInstance().createSparkJobServerClient(jobserverUrl);
            // GET /jars
            List<SparkJobJarInfo> jarInfos = client.getJars();
            for (SparkJobJarInfo jarInfo : jarInfos) {
                System.out.println("--> " + jarInfo.toString());
            }

            // POST /jars/<appName>
            //client.uploadSparkJobJar(LivyUtils.getJarFile(com.forsrc.spark.job.WordCount.class), "spark-test");

            // GET /contexts
            List<String> contexts = client.getContexts();
            System.out.println("--> Current contexts:");
            for (String cxt : contexts) {
                System.out.println("--> " + cxt);
            }

            // DELETE /contexts/<name>
            client.deleteContext("cxtTest");
            // POST /contexts/<name>--Create context with name ctxTest and null parameter
            // client.createContext("ctxTest", null);
            // POST /contexts/<name>--Create context with parameters
            Map<String, String> params = new HashMap<String, String>();
            //params.put(ISparkJobServerClientConstants.PARAM_MEM_PER_NODE, "512m");
            //params.put(ISparkJobServerClientConstants.PARAM_NUM_CPU_CORES, "10");
            client.createContext("cxtTest", params);

            

            
            SparkJobResult result = null;

            // GET /jobs
            List<SparkJobInfo> jobInfos = client.getJobs();
            System.out.println("Current jobs:");
            for (SparkJobInfo jobInfo : jobInfos) {
                System.out.println("--> " + jobInfo);
                result = client.getJobResult(jobInfo.getJobId());
                System.out.println("--> " + result);
            }

            // Post /jobs---Create a new job
            File jar = JarFileUtils.getJarFile(com.forsrc.spark.job.WordCount.class);
            params.put(ISparkJobServerClientConstants.PARAM_APP_NAME, jar.getName());
            params.put(ISparkJobServerClientConstants.PARAM_CLASS_PATH, "spark.jobserver.WordCountExample");
            // 1.start a spark job asynchronously and just get the status information
           // result = client.startJob("input.string= A B C D A B C D ABCD A B A", params);
           // System.out.println("-->1 " + result);

            // 2.start a spark job synchronously and wait until the result
            params.put(ISparkJobServerClientConstants.PARAM_CONTEXT, "cxtTest");
            params.put(ISparkJobServerClientConstants.PARAM_SYNC, "true");
            result = client.startJob("input.string= A B C D A B C D ABCD A B A", params);
            System.out.println("-->2 " + result);

            // GET /jobs/<jobId>---Gets the result or status of a specific job
            //result = client.getJobResult("A");
            //System.out.println("-->3 " + result);

            // GET /jobs/<jobId>/config - Gets the job configuration
            //SparkJobConfig jobConfig = client.getConfig("A");
            //System.out.println("--> " + jobConfig);
            return result;
        } catch (SparkJobServerClientException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String jobserverUpdatejar(Class<?> cls) {
        ISparkJobServerClient client = null;
        try {
            client = SparkJobServerClientFactory.getInstance().createSparkJobServerClient(jobserverUrl);
            // GET /jars
            List<SparkJobJarInfo> jarInfos = client.getJars();
            for (SparkJobJarInfo jarInfo : jarInfos) {
                System.out.println("--> " + jarInfo.toString());
            }

            File jar = JarFileUtils.getJarFile(cls);
            // POST /jars/<appName>
            client.uploadSparkJobJar(jar, jar.getName());

        } catch (Exception e) {
            return e.getMessage();
        }
        return "OK";
    }

}
