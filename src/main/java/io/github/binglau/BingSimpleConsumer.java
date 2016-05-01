package io.github.binglau;

import kafka.api.*;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 1. 从所有活跃的broker中找出哪个是指定Topic（主题） Partition（分区）中的leader broker
 * 2. 找出指定Topic Partition中的所有备份broker
 * 3. 构造请求
 * 4. 发送请求获取数据
 * 5. 处理leader broker变更
 */
public class BingSimpleConsumer {

    public void consumer() {
        int partition = 0;
        BrokerEndPoint leaderBroker = findLeader(Config.KAFKA_ADDRESS, Config.TOPIC, partition);

        SimpleConsumer simpleConsumer = new SimpleConsumer(
                leaderBroker.host(), leaderBroker.port(), 20000, 10000, "bingSimpleConsumer");
        long startOffset = 1;
        int fetchSize = 1000;

        while (true) {
            long offset = startOffset;
            FetchRequest request = new FetchRequestBuilder()
                    .addFetch(Config.TOPIC, 0, startOffset, fetchSize).build();

            FetchResponse response = simpleConsumer.fetch(request);

            ByteBufferMessageSet messageSet = response.messageSet(Config.TOPIC, partition);
            for (MessageAndOffset messageAndOffset : messageSet) {
                Message msg = messageAndOffset.message();
                ByteBuffer payload = msg.payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                String message = new String(bytes);
                offset = messageAndOffset.offset();
                System.out.println("partition : " + 3 + ", offset : " + offset + "  msg : " + message);
            }
            startOffset = offset + 1;
        }

    }

    public BrokerEndPoint findLeader(String brokerHosts, String topic, int partition) {
        BrokerEndPoint leader = findPartitionMetadata(brokerHosts, topic, partition).leader();
        System.out.println(String.format("Leader tor topic %s, partition %d is %s:%d",
                topic, partition, leader.host(), leader.port()));
        return leader;
    }

    private PartitionMetadata findPartitionMetadata(String brokerHosts, String topic, int partition) {
        PartitionMetadata returnMetadata = null;
        for (String brokerHost : brokerHosts.split(",")) {
            SimpleConsumer consumer = null;
            String[] splits = brokerHost.split(":");
            consumer = new SimpleConsumer(
                    splits[0], Integer.valueOf(splits[1]), 100000, 64 * 1024, "leaderLookup"
            );
            List<String> topics = Collections.singletonList(topic);
            TopicMetadataRequest request = new TopicMetadataRequest(topics);
            TopicMetadataResponse response = consumer.send(request);
            List<TopicMetadata> topicMetadatas = response.topicsMetadata();
            for (TopicMetadata topicMetadata: topicMetadatas) {
                for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
                    returnMetadata = partitionMetadata;
                }
            }
            if (consumer != null) {
                consumer.close();
            }
        }
        return returnMetadata;
    }

    public long getLastOffset(SimpleConsumer consumer, String topic,
                              int partition, String clientID, long whichTime) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
                new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientID);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    public static void main( String[] args ) {
        new BingSimpleConsumer().consumer();
    }
}
