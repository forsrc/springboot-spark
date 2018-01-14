package com.forsrc.spark.web.wordcount.controller;

import java.io.FileNotFoundException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import com.forsrc.spark.web.wordcount.service.WordCountService;

@RestController
@RequestMapping(value = "/spark")
public class WordCountController {

    private static final Logger logger = LoggerFactory.getLogger(WordCountController.class);

    @Autowired
    private WordCountService wordCountService;

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
        Map<String, Integer> map = wordCountService.wordCount(ResourceUtils.getFile("classpath:WordCount.txt").getAbsolutePath());
        return new ResponseEntity<>(map, HttpStatus.OK);
    }

    @RequestMapping(value = "/wordcount/{word}", method = { RequestMethod.GET, RequestMethod.POST }, produces = {
            MediaType.APPLICATION_JSON_UTF8_VALUE })
    public ResponseEntity<Map<String, Integer>> count(@PathVariable("word") String word,UriComponentsBuilder ucBuilder) throws FileNotFoundException {
        Map<String, Integer> map = new HashMap<>();
        int count = wordCountService.count(ResourceUtils.getFile("classpath:WordCount.txt").getAbsolutePath(), word);
        map.put(word, count);
        return new ResponseEntity<>(map, HttpStatus.OK);
    }

    @RequestMapping(value = "/livy/wordcount/{word}", method = { RequestMethod.GET, RequestMethod.POST }, produces = {
            MediaType.APPLICATION_JSON_UTF8_VALUE })
    public ResponseEntity<Map<String, Integer>> livycount(@PathVariable("word") String word, UriComponentsBuilder ucBuilder) throws FileNotFoundException {
        Map<String, Integer> map = new HashMap<>();
        int count = wordCountService.livyCount(ResourceUtils.getFile("classpath:WordCount.txt").getAbsolutePath(), word);
        map.put(word, count);
        return new ResponseEntity<>(map, HttpStatus.OK);
    }

    @RequestMapping(value = "/livy/helloworld", method = { RequestMethod.GET, RequestMethod.POST }, produces = {
            MediaType.APPLICATION_JSON_UTF8_VALUE })
    public ResponseEntity<Map<String, String>> livyHelloworld(UriComponentsBuilder ucBuilder) throws FileNotFoundException {
        Map<String, String> map = new HashMap<>();
        String message = wordCountService.livyHelloworld();
        map.put("message", message);
        return new ResponseEntity<>(map, HttpStatus.OK);
    }

    @RequestMapping(value = "/jobserver/helloworld", method = { RequestMethod.GET, RequestMethod.POST }, produces = {
            MediaType.APPLICATION_JSON_UTF8_VALUE })
    public ResponseEntity<Map<String, String>> jobserverHelloworld(UriComponentsBuilder ucBuilder) throws Exception {
        Map<String, String> map = new HashMap<>();
        String message = wordCountService.jobserverHelloworld();
        map.put("message", message);
        return new ResponseEntity<>(map, HttpStatus.OK);
    }

    @RequestMapping(value = "/jobserver/updatejar", method = { RequestMethod.GET, RequestMethod.POST }, produces = {
            MediaType.APPLICATION_JSON_UTF8_VALUE })
    public ResponseEntity<Map<String, Object>> updatejar(UriComponentsBuilder ucBuilder) throws Exception {
        Map<String, Object> map = wordCountService.jobserverUpdatejar(com.forsrc.spark.job.WordCount.class);
        return new ResponseEntity<>(map, HttpStatus.OK);
    }
}
