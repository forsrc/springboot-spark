package com.forsrc.spark.web.wordcount.service;

import org.apache.spark.deploy.rest.SubmitRestProtocolResponse;
import org.springframework.stereotype.Service;

@Service
public interface UploadJarService {

    public SubmitRestProtocolResponse upload(String url, String jar, String appName, String appClass, String[] args);
}
