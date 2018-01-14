package com.forsrc.spark.web.wordcount.service.impl;

import static org.apache.spark.sql.functions.col;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.yarn.webapp.example.HelloWorld;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
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
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

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
    public Map<String, Object> jobserverWordCountExample() throws Exception {
        File jar = JarFileUtils.getJarFile(spark.jobserver.WordCountExample.class);
        Map<String, Object> map = Collections.emptyMap();
        HttpPost postMethod = new HttpPost(String
                .format("%s/jobs?appName=%s&classPath=spark.jobserver.WordCountExample&sync=true", jobserverUrl, jar.getName()));
        final CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        try {
            StringEntity strEntity = new StringEntity("input.string = a b c a b see");
            strEntity.setContentEncoding("UTF-8");
            strEntity.setContentType("text/plain");
            postMethod.setEntity(strEntity);
            HttpResponse response = httpClient.execute(postMethod);
            int statusCode = response.getStatusLine().getStatusCode();
            System.out.println(String.format("statusCode: %s; %s", statusCode, response));

            map = objectMapper.readValue(IOUtils.toString(response.getEntity().getContent(), "UTF-8"),
                    new TypeReference<HashMap<String, Object>>() {
                    });

            map.put("statusCode", statusCode);

        } catch (Exception e) {
            throw e;
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
        return map;
    }

    @Override
    public Map<String, Object> jobserverUpdatejar(Class<?> cls) throws Exception {

        File jar = JarFileUtils.getJarFile(cls);
        System.out.println(jar);
        // POST /jars/<appName>
        Map<String, Object> map = Collections.emptyMap();
        HttpPost postMethod = new HttpPost(jobserverUrl + "/jars/" + jar.getName());
        final CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        try {
            ByteArrayEntity entity = new ByteArrayEntity(FileUtils.readFileToByteArray(jar));
            postMethod.setEntity(entity);
            entity.setContentType("application/java-archive");
            HttpResponse response = httpClient.execute(postMethod);
            int statusCode = response.getStatusLine().getStatusCode();

            map = objectMapper.readValue(IOUtils.toString(response.getEntity().getContent(), "UTF-8"),
                    new TypeReference<HashMap<String, Object>>() {
                    });
            map.put("statusCode", statusCode);

        } catch (Exception e) {
            throw e;
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
        return map;
    }

    public Map<String, Object> jobserverUpdatejar1(Class<?> cls) throws Exception {

        File jar = JarFileUtils.getJarFile(cls);
        System.out.println(jar);
        // POST /jars/<appName>

        Map<String, String> vars = new HashMap<String, String>();
        vars.put("appName", jar.getName());

        MultiValueMap<String, Object> bodyMap = new LinkedMultiValueMap<>();
        bodyMap.add("jar", new FileSystemResource(jar));

        // bodyMap.add("fileName", jar.getName());
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        String cd = "attachment; filename=\"" + jar.getName() + "\"";
        headers.add("Content-Disposition", cd);
        headers.add(HttpHeaders.ACCEPT, "*/*");
        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(bodyMap, headers);

        ResponseEntity<String> response = restTemplate.exchange(jobserverUrl + "/jars/1" + jar.getName(),
                HttpMethod.POST, requestEntity, String.class);

        Map<String, Object> map = objectMapper.readValue(response.getBody(),
                new TypeReference<HashMap<String, Object>>() {
                });
        return map;
    }
}
