package com.forsrc.spark.web.wordcount.controller;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.deploy.rest.SubmitRestProtocolResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import com.forsrc.spark.web.wordcount.service.LivyWordCountService;
import com.forsrc.spark.web.wordcount.service.UploadJarService;
import com.forsrc.spark.web.wordcount.service.WordCountService;

@RestController
@RequestMapping(value = "/spark")
public class WordCountController {

    private static final Logger logger = LoggerFactory.getLogger(WordCountController.class);

    @Autowired
    private WordCountService wordCountService;

    @Autowired
    private LivyWordCountService livyWordCountService;

    @Autowired
    private UploadJarService uploadJarService;

    @RequestMapping(value = "/test", method = { RequestMethod.GET, RequestMethod.POST }, produces = {
            MediaType.APPLICATION_JSON_UTF8_VALUE })
    public ResponseEntity<Map<String, String>> get(UriComponentsBuilder ucBuilder) {
        Map<String, String> map = new HashMap<>();
        map.put("test", "hello world");
        return new ResponseEntity<>(map, HttpStatus.OK);
    }

    @RequestMapping(value = "/wordcount", method = { RequestMethod.GET, RequestMethod.POST }, produces = {
            MediaType.APPLICATION_JSON_UTF8_VALUE })
    public ResponseEntity<Map<String, Integer>> wordCount(UriComponentsBuilder ucBuilder) throws FileNotFoundException {
        Map<String, Integer> map = wordCountService
                .wordCount(ResourceUtils.getFile("classpath:WordCount.txt").getAbsolutePath());
        return new ResponseEntity<>(map, HttpStatus.OK);
    }

    @RequestMapping(value = "/wordcount/{word}", method = { RequestMethod.GET, RequestMethod.POST }, produces = {
            MediaType.APPLICATION_JSON_UTF8_VALUE })
    public ResponseEntity<Map<String, Integer>> count(@PathVariable("word") String word, UriComponentsBuilder ucBuilder)
            throws FileNotFoundException {
        Map<String, Integer> map = new HashMap<>();
        int count = wordCountService.count(ResourceUtils.getFile("classpath:WordCount.txt").getAbsolutePath(), word);
        map.put(word, count);
        return new ResponseEntity<>(map, HttpStatus.OK);
    }

    @RequestMapping(value = "/livy/wordcount/{word}", method = { RequestMethod.GET, RequestMethod.POST }, produces = {
            MediaType.APPLICATION_JSON_UTF8_VALUE })
    public ResponseEntity<Map<String, Integer>> livycount(@PathVariable("word") String word,
            UriComponentsBuilder ucBuilder) throws FileNotFoundException {
        Map<String, Integer> map = new HashMap<>();
        int count = livyWordCountService.livyCount(ResourceUtils.getFile("classpath:WordCount.txt").getAbsolutePath(),
                word);
        map.put(word, count);
        return new ResponseEntity<>(map, HttpStatus.OK);
    }

    @RequestMapping(value = "/upload", method = { RequestMethod.GET, RequestMethod.POST }, produces = {
            MediaType.APPLICATION_JSON_UTF8_VALUE })
    public ResponseEntity<SubmitRestProtocolResponse> upload(@RequestParam("jar") String jar, @RequestParam("appClass") String appClass,
            @RequestParam("appName") String appName,  @RequestParam("url") String url, UriComponentsBuilder ucBuilder) throws FileNotFoundException {
        SubmitRestProtocolResponse srpr = uploadJarService.upload(url, jar, appName, appClass, new String[] {});
        return new ResponseEntity<>(srpr, HttpStatus.OK);
    }
}
