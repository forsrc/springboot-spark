package com.forsrc.spark.web.wordcount.service;

import org.springframework.stereotype.Service;


@Service
public interface LivyWordCountService {


    public int livyCount(String filename, String str);
}
