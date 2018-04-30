package com.forsrc.spark.web.wordcount.service.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.deploy.master.DriverState;
import org.apache.spark.deploy.rest.CreateSubmissionRequest;
import org.apache.spark.deploy.rest.RestSubmissionClient;
import org.apache.spark.deploy.rest.SubmitRestProtocolResponse;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forsrc.spark.web.wordcount.service.UploadJarService;

import scala.Predef$;
import scala.collection.JavaConverters$;

@Service
public class UploadJarServiceImpl implements UploadJarService{



    @Override
    public SubmitRestProtocolResponse upload(String url, String jar, String appName, String appClass, String[] args) {
        RestSubmissionClient rsc = new RestSubmissionClient(url);
        Map<String, String> sparkProps = new HashMap<>();
        sparkProps.put("spark.app.name", appName);
        sparkProps.put("spark.master", "local[*]");
        sparkProps.put("spark.jars", jar);


        scala.collection.immutable.Map<String, String> envMap =
                JavaConverters$.MODULE$.mapAsScalaMapConverter(new HashMap<String, String>()).asScala()
                        .toMap(Predef$.MODULE$.<scala.Tuple2<String, String>>conforms());
        scala.collection.immutable.Map<String, String> propsMap =
                JavaConverters$.MODULE$.mapAsScalaMapConverter(sparkProps).asScala()
                        .toMap(Predef$.MODULE$.<scala.Tuple2<String, String>>conforms());

        CreateSubmissionRequest csr = rsc.constructSubmitRequest(
                jar,
                appClass,
                args,
                propsMap,
                envMap);

        SubmitRestProtocolResponse resp = rsc.createSubmission(csr);

        String submissionId = getJsonProperty(resp.toJson(), "submissionId");


        String appState;
        SubmitRestProtocolResponse stat = null;
        while (true) {
            try {
                Thread.sleep(3);
            } catch (InterruptedException e) {
            }
            stat = rsc.requestSubmissionStatus(submissionId, false);
            appState = getJsonProperty(stat.toJson(), "driverState");
            if (!(appState.equals(DriverState.SUBMITTED().toString()) ||
                    appState.equals(DriverState.RUNNING().toString()) ||
                    appState.equals(DriverState.RELAUNCHING().toString()) ||
                    appState.equals(DriverState.UNKNOWN().toString()))) {
                System.out.println("Spark App completed with status: " + appState);
                break;
            }
        }
        if (!appState.equals(DriverState.FINISHED().toString())) {
            //throw new RuntimeException("Spark App submission " + submissionId + " failed with status " + appState);
        }
        return stat;
    }

    private String getJsonProperty(String json, String prop) {
        try {
            HashMap<String, Object> props =
                    new ObjectMapper().readValue(json, HashMap.class);
            return props.get(prop).toString();
        } catch (IOException ioe) {
            return null;
        }
    }


}
