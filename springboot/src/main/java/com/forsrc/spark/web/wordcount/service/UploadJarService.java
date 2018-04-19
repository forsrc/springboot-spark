package com.forsrc.spark.web.wordcount.service;

import org.springframework.stereotype.Service;

@Service
public interface UploadJarService {

    public void upload(String url, String jar, String appName, String appClass, String[] args);
}
