package com.forsrc.spark.web.wordcount.controller;

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
    public ResponseEntity<Map<String, Integer>> wordCount(UriComponentsBuilder ucBuilder) {
        Map<String, Integer> map = wordCountService.wordCount(new ClassPathResource("WordCount.txt").getPath());
        return new ResponseEntity<>(map, HttpStatus.OK);
    }
}
