package com.forsrc.spark.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobHandle;
import com.cloudera.livy.LivyClient;
import com.cloudera.livy.LivyClientBuilder;
import com.forsrc.spark.livy.job.WordCountJob;

public class JarFileUtils {

    public static interface Handler {
        public void handle(LivyClient livyClient);
    }

    public static <T> T handle(String livyUrl, Class<?> cls, Job<T> job)
            throws IOException, URISyntaxException, InterruptedException, ExecutionException {
        LivyClient livyClient = null;
        try {
            livyClient = getLivyClient(livyUrl);
            //livyClient.uploadFile(getJarFile(cls)).get();

            JobHandle<T> handle = livyClient.submit(job);
            return handle.get();
        } catch (IOException e) {
            throw e;
        } catch (URISyntaxException e) {
            throw e;
        } catch (InterruptedException e) {
            throw e;
        } catch (ExecutionException e) {
            throw e;
        } finally {
            if (livyClient != null) {
                livyClient.stop(true);
            }
        }

    }

    public static LivyClient getLivyClient(String livyUrl) throws IOException, URISyntaxException {
        return new LivyClientBuilder(true).setURI(new URI(livyUrl)).build();
    }

    public static File getJarFile(Class<?> cls) throws FileNotFoundException {
        String file = cls.getProtectionDomain().getCodeSource().getLocation().getFile();
        if (file.endsWith(".jar")) {
            return new File(file);
        }
        if (file.endsWith("/")) {
            file = file.substring(0, file.lastIndexOf("/"));
        }
        String path = file.substring(0, file.lastIndexOf("/") + 1);

        File[] files = new File(path).listFiles();
        for (File f : files) {
            if (f.getName().startsWith("forsrc-springboot-spark-job") && f.getName().endsWith(".jar")) {
                return f;
            }
        }
        throw new FileNotFoundException("not found jar for " + cls.getName());
    }
}
