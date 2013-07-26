package com.infochimps.storm.testrig.util;

import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.utils.Utils;

public class KafkaConsumer {

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

        // create a consumer to connect to the kafka server running on
        // localhost, port 9092, socket timeout of 10 secs, socket receive
        // buffer of ~1MB
        SimpleConsumer consumer = new SimpleConsumer("127.0.0.1", 9092, 10000, 1024000);

        long offset = 0;
        while (true) {
            // create a fetch request for topic “test”, partition 0, current
            // offset, and fetch size of 1MB
            FetchRequest fetchRequest = new FetchRequest("testrig3", 1, offset, 1000000);

            // get the message set from the consumer and print them out
            ByteBufferMessageSet messages = consumer.fetch(fetchRequest);
            for (MessageAndOffset msg : messages) {
                System.out.println("consumed: "
                        + Utils.toString(msg.message().payload(), "UTF-8"));
                // advance the offset after consuming each message
                offset = msg.offset();
            }
        }

    }
}
