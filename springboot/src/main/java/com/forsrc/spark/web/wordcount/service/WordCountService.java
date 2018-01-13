package com.forsrc.spark.web.wordcount.service;

import java.util.Map;

import org.springframework.stereotype.Service;


@Service
public interface WordCountService {

    public Map<String, Integer> wordCount(String filename);

    public int count(String filename, String str);

    public int livyCount(String filename, String str);

    public String livyHelloworld();

    public String jobserverHelloworld();
}
