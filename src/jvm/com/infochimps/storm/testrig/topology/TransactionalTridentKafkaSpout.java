package com.infochimps.storm.testrig.topology;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.DynamicPartitionConnections;
import storm.kafka.GlobalPartitionId;
import storm.kafka.trident.FailedFetchException;
import storm.kafka.trident.IBrokerReader;
import storm.kafka.trident.KafkaUtils;
import storm.kafka.trident.MaxMetric;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.Config;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.google.common.collect.ImmutableMap;
import com.infochimps.storm.testrig.util.KafkaLoader;

public class TransactionalTridentKafkaSpout implements
        IPartitionedTridentSpout<Map<String, List>, GlobalPartitionId, Map> {

    TridentKafkaConfig _config;
    String _topologyInstanceId = UUID.randomUUID().toString();
    public static final Logger LOG = LoggerFactory
            .getLogger(TransactionalTridentKafkaSpout.class);

    public TransactionalTridentKafkaSpout(TridentKafkaConfig config) {
        _config = config;
    }

    class Coordinator implements IPartitionedTridentSpout.Coordinator<Map> {
        IBrokerReader reader;

        public Coordinator(Map conf) {
            reader = KafkaUtils.makeBrokerReader(conf, _config);
        }

        @Override
        public void close() {
            _config.coordinator.close();
        }

        @Override
        public boolean isReady(long txid) {
            return _config.coordinator.isReady(txid);
        }

        @Override
        public Map getPartitionsForBatch() {
            return reader.getCurrentBrokers();
        }
    }

    class Emitter
            implements
            IPartitionedTridentSpout.Emitter<Map<String, List>, GlobalPartitionId, Map> {
        DynamicPartitionConnections _connections;
        String _topologyName;
        TopologyContext _context;
        KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric;
        ReducedMetric _kafkaMeanFetchLatencyMetric;
        CombinedMetric _kafkaMaxFetchLatencyMetric;

        public Emitter(Map conf, TopologyContext context) {
            _connections = new DynamicPartitionConnections(_config);
            _topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
            _context = context;
            _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(_config.topic, _connections);
            context.registerMetric("kafkaOffset", _kafkaOffsetMetric, 60);
            _kafkaMeanFetchLatencyMetric = context
                    .registerMetric("kafkaFetchAvg", new MeanReducer(), 60);
            _kafkaMaxFetchLatencyMetric = context
                    .registerMetric("kafkaFetchMax", new MaxMetric(), 60);
        }

        @Override
        public Map emitPartitionBatchNew(TransactionAttempt attempt, TridentCollector collector, GlobalPartitionId partition, Map lastMeta) {
            SimpleConsumer consumer = _connections.register(partition);
            Map ret = emitPartitionBatchNew(attempt, _config, consumer, partition, collector, lastMeta, _topologyInstanceId, _topologyName, _kafkaMeanFetchLatencyMetric, _kafkaMaxFetchLatencyMetric);
            _kafkaOffsetMetric.setLatestEmittedOffset(partition, (Long) ret
                    .get("offset"));
            return ret;
        }

        public Map emitPartitionBatchNew(TransactionAttempt attempt, TridentKafkaConfig config, SimpleConsumer consumer, GlobalPartitionId partition, TridentCollector collector, Map lastMeta, String topologyInstanceId, String topologyName, ReducedMetric meanMetric, CombinedMetric maxMetric) {
            long offset;
            if (lastMeta != null) {
                String lastInstanceId = null;
                Map lastTopoMeta = (Map) lastMeta.get("topology");
                if (lastTopoMeta != null) {
                    lastInstanceId = (String) lastTopoMeta.get("id");
                }
                if (config.forceFromStart
                        && !topologyInstanceId.equals(lastInstanceId)) {
                    offset = consumer
                            .getOffsetsBefore(config.topic, partition.partition, config.startOffsetTime, 1)[0];
                } else {
                    offset = (Long) lastMeta.get("nextOffset");
                }
            } else {
                long startTime = -1;
                if (config.forceFromStart) {
                    startTime = config.startOffsetTime;
                }
                offset = consumer
                        .getOffsetsBefore(config.topic, partition.partition, startTime, 1)[0];
            }
            ByteBufferMessageSet msgs;
            boolean done = false;
            long endoffset = 0;
            String t = config.topic;
            String p = "" + partition.partition;
            String tx = attempt.getTransactionId() + ":"
                    + attempt.getAttemptId();
            System.out
                    .printf("--While starts for topic: [%s] partition: [%s] txn: [%s]\n", t, p, tx);
            while (!done) {

                try {
                    long start = System.nanoTime();
                    msgs = consumer
                            .fetch(new FetchRequest(config.topic, partition.partition, offset, config.fetchSizeBytes));
                    // System.out.println("topic: [" + config.topic
                    // + "] partition: [" + partition.partition
                    // + "] msg: " + msgs.sizeInBytes());
                    System.out
                            .printf("--Getting data from Kafka topic: [%s] partition: [%s] txn: [%s] msg: %s\n", t, p, tx, msgs
                                    .validBytes());
                    long end = System.nanoTime();
                    long millis = (end - start) / 1000000;
                    meanMetric.update(millis);
                    maxMetric.update(millis);
                } catch (Exception e) {
                    if (e instanceof ConnectException) {
                        throw new FailedFetchException(e);
                    } else {
                        throw new RuntimeException(e);
                    }
                }
                endoffset = offset;
                if (msgs.validBytes() == 0) {
                    break;
                }
                for (MessageAndOffset msg : msgs) {
                    if (done) {
                        break;
                    }
                    Iterable<List<Object>> values = config.scheme
                            .deserialize(Utils.toByteArray(msg.message()
                                    .payload()));
                    // System.out.println("topic: " + config.topic
                    // + " partition: " + partition.partition + "msg: "
                    // + values);
                    if (values != null) {
                        for (List<Object> value : values) {
                            String v = (String) value.get(0);
                            System.out
                                    .printf("--Value topic: [%s] partition: [%s] txn: [%s] v: %s\n", t, p, tx, v);
                            if (v.equals(KafkaLoader.STOP)) {
                                done = true;
                                System.out
                                        .printf("--Batch over: [%s] partition: [%s] txn: [%s] v: %s\n", t, p, tx, v);

                            }
                            if (done) {
                                break;
                            }
                            collector.emit(value);
                        }
                    } else {
                        System.out
                                .printf("--No data from Kafka: [%s] partition: [%s] txn: [%s] \n", t, p, tx);
                        done = true;
                        break;
                    }

                    endoffset = msg.offset();
                }

            }
            System.out
                    .printf("--While ends topic: [%s] partition: [%s] txn: [%s] \n", t, p, tx);

            Map newMeta = new HashMap();
            newMeta.put("offset", offset);
            newMeta.put("nextOffset", endoffset);
            newMeta.put("instanceId", topologyInstanceId);
            newMeta.put("partition", partition.partition);
            newMeta.put("broker", ImmutableMap
                    .of("host", partition.host.host, "port", partition.host.port));
            newMeta.put("topic", config.topic);
            newMeta.put("topology", ImmutableMap
                    .of("name", topologyName, "id", topologyInstanceId));
            return newMeta;
        }

        @Override
        public void emitPartitionBatch(TransactionAttempt attempt, TridentCollector collector, GlobalPartitionId partition, Map meta) {
            String instanceId = (String) meta.get("instanceId");
            if (!_config.forceFromStart
                    || instanceId.equals(_topologyInstanceId)) {
                String t = _config.topic;
                String p = "" + partition.partition;
                String tx = attempt.getTransactionId() + ":"
                        + attempt.getAttemptId();
                System.out
                        .printf("--Retry for topic: [%s] partition: [%s] txn: [%s]\n", t, p, tx);
                SimpleConsumer consumer = _connections.register(partition);
                long offset = (Long) meta.get("offset");
                long nextOffset = (Long) meta.get("nextOffset");
                long start = System.nanoTime();
                ByteBufferMessageSet msgs = consumer
                        .fetch(new FetchRequest(_config.topic, partition.partition, offset, _config.fetchSizeBytes));
                long end = System.nanoTime();
                long millis = (end - start) / 1000000;
                _kafkaMeanFetchLatencyMetric.update(millis);
                _kafkaMaxFetchLatencyMetric.update(millis);

                for (MessageAndOffset msg : msgs) {
                    if (offset == nextOffset) {
                        break;
                    }
                    if (offset > nextOffset) {
                        throw new RuntimeException("Error when re-emitting batch. overshot the end offset");
                    }
                    // KafkaUtils.emit(_config, collector, msg.message());
                    Iterable<List<Object>> values = _config.scheme
                            .deserialize(Utils.toByteArray(msg.message()
                                    .payload()));
                    // System.out.println("topic: " + config.topic
                    // + " partition: " + partition.partition + "msg: "
                    // + values);
                    if (values != null) {
                        for (List<Object> value : values) {
                            String v = (String) value.get(0);
                            System.out
                                    .printf("--Value topic: [%s] partition: [%s] txn: [%s] v: %s\n", t, p, tx, v);
                            if (v.equals(KafkaLoader.STOP)) {
                                System.out
                                        .printf("--Batch over: [%s] partition: [%s] txn: [%s] v: %s\n", t, p, tx, v);

                            }
                            collector.emit(value);
                        }
                    }
                    offset = msg.offset();
                }
            }
        }

        @Override
        public void close() {
            _connections.clear();
        }

        @Override
        public List<GlobalPartitionId> getOrderedPartitions(Map<String, List> partitions) {
            return KafkaUtils.getOrderedPartitions(partitions);
        }

        @Override
        public void refreshPartitions(List<GlobalPartitionId> list) {
            _connections.clear();
            _kafkaOffsetMetric
                    .refreshPartitions(new HashSet<GlobalPartitionId>(list));
        }
    }

    @Override
    public IPartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new Coordinator(conf);
    }

    @Override
    public IPartitionedTridentSpout.Emitter getEmitter(Map conf, TopologyContext context) {
        return new Emitter(conf, context);
    }

    @Override
    public Fields getOutputFields() {
        return _config.scheme.getOutputFields();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}