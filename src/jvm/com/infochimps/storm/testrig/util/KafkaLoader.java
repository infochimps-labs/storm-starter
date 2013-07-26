package com.infochimps.storm.testrig.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

public class KafkaLoader {

    public static final String START = "___START___";
    public static final String STOP = "___STOP___";

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("zk.connect", "127.0.0.1:2181");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "com.infochimps.storm.testrig.util.StaticPartitioner");

        ProducerConfig config = new ProducerConfig(props);
        Producer<Integer, String> producer = new Producer<Integer, String>(config);

        String filename = "/Users/sa/code/data.txt";
        BufferedReader file = new BufferedReader(new FileReader(filename));
        String line;
        int length = 3;
        for (int i = 0; i < length; i++) {
            ArrayList<String> a = new ArrayList<String>();
            a.add(START);
            while ((line = file.readLine()) != null) {
                System.out.println(i + line);
                a.add(i + line);
            }
            a.add(STOP);
            ProducerData<Integer, String> data = new ProducerData<Integer, String>("testrig3", i, a);
            producer.send(data);
            file.close();
            file = new BufferedReader(new FileReader(filename));
        }
        file.close();
    }

}