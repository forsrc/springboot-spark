package com.forsrc.spark.livy.job;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;

import static org.apache.spark.sql.functions.col;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class HelloWorldJob implements Job<String> {


    @Override
    public String call(JobContext jobContext) throws Exception {

        SparkSession sparkSession = jobContext.sparkSession();

        return "hello world -> " + sparkSession.logName() + " -> " + new Date();
    }


}
