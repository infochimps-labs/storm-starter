package com.infochimps.storm.testrig.util;

import kafka.producer.Partitioner;

public class StaticPartitioner implements Partitioner<Integer> {

        @Override
        public int partition(Integer arg0, int arg1) {
            // TODO Auto-generated method stub
            return arg0;
        }

    }