package com.forsrc.spark.web.wordcount.service.impl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.forsrc.spark.livy.job.WordCountJob;
import com.forsrc.spark.utils.LivyUtils;
import com.forsrc.spark.web.wordcount.service.LivyWordCountService;

@Service
public class LivyWordCountServiceImpl implements LivyWordCountService {

    @Value("${livy.url}")
    private String livyUrl;

    @Override
    public int livyCount(String filename, String word) {

        try {
            return LivyUtils.handle(livyUrl, WordCountJob.class, new WordCountJob(filename, word));
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

    }

}
