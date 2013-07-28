package com.infochimps.storm.testrig.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

public class KafkaLoader {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("zk.connect", "127.0.0.1:2181");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        String filename = "/Users/sa/code/pagecounts-20080228-020001";
        BufferedReader file = new BufferedReader(new FileReader(filename));
        String line;
        int length = 10;
        for (int i = 0; i < length; i++) {

            if ((line = file.readLine()) != null) {
                System.out.println(line);
                ProducerData<String, String> data = new ProducerData<String, String>(
                        "testrig3", line);
                producer.send(data);
            }
        }
        file.close();
    }
}