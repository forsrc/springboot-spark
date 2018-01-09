package com.forsrc.spark.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.util.ResourceUtils;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;


@Configuration
public class SparkConfig {

    @Value("${spark.appName}")
    private String appName;

    @Value("${spark.home}")
    private String home;

    @Value("${spark.master}")
    private String master;

    @Bean
    public SparkConf sparkConf() throws FileNotFoundException {

        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                .setSparkHome(System.getenv("SPARK_HOME"))
                .setMaster(master)
                //.set("spark.testing.memory", "2147480000")
                ;
        if (home != null) {
            sparkConf.setSparkHome(home);
        }
        return sparkConf;
    }

    @Bean
    public JavaSparkContext javaSparkContext() throws FileNotFoundException {
        return new JavaSparkContext(sparkConf());
    }

    @Bean
    public SparkSession sparkSession() throws FileNotFoundException {
        return SparkSession
                .builder()
                .sparkContext(javaSparkContext().sc())
                .appName("springboot-spark-session")
                .getOrCreate();
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }
}