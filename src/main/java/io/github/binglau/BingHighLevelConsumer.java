package io.github.binglau;


import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 类BingHighLevelConsumer.java的实现描述：TODO:类实现描述
 *
 * @author bingjian.lbj 2016-04-30 下午9:51
 */
public class BingHighLevelConsumer {
    private String groupId;
    private String consumerId;
    private ExecutorService executor;


    private int threadCountPerTopic;

    public BingHighLevelConsumer(String groupId, String consumerId, int threadCountPerTopic) {
        this.consumerId = consumerId;
        this.groupId = groupId;
        this.threadCountPerTopic = threadCountPerTopic;
    }

    private ConsumerConfig config(){
        Properties props = new Properties();
        props.put("group.id", groupId);
        props.put("consumer.id", consumerId);
        props.put("zookeeper.connect", Config.ZK_CONNECT);
        props.put("zookeeper.session.timeout.ms", "60000");
        props.put("zookeeper.sync.time.ms", "2000");
        return new ConsumerConfig(props);
    }

    private ConsumerConnector getConn(){
        return Consumer.createJavaConsumerConnector(config());
    }

    public void consume() {
        ConsumerConnector connector = getConn();

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

        // 设置每个topic开几个线程
        topicCountMap.put(Config.TOPIC, threadCountPerTopic);
        executor = Executors.newFixedThreadPool(threadCountPerTopic);

        // 获取stream
        Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topicCountMap);

        // 为每个stream启动一个线程消费消息
        for (KafkaStream<byte[], byte[]> stream : streams.get(Config.TOPIC)) {
            BingStreamThread bingStreamThread = new BingStreamThread(stream);
            executor.submit(bingStreamThread);
        }
    }

    private class BingStreamThread implements Runnable{
        private KafkaStream<byte[], byte[]> stream;

        public BingStreamThread(KafkaStream<byte[], byte[]> stream){
           this.stream = stream;
        }
        public void run() {
            ConsumerIterator<byte[], byte[]> streamIterator = stream.iterator();

            // 逐条处理消息
            while (streamIterator.hasNext()) {
                MessageAndMetadata<byte[], byte[]> message = streamIterator.next();
                String topic = message.topic();
                int partition = message.partition();
                long offset = message.offset();
                String key = new String(message.key());
                String msg = new String(message.message());
                // 在这里处理消息,这里仅简单的输出
                // 如果消息消费失败，可以将已上信息打印到日志中，活着发送到报警短信和邮件中，以便后续处理
                System.out.println("consumerId:" + consumerId
                        + ", thread : " + Thread.currentThread().getName()
                        + ", topic : " + topic
                        + ", partition : " + partition
                        + ", offset : " + offset
                        + " , key : " + key
                        + " , mess : " + msg);
            }
        }
    }

    public static void main(String[] args){
        String groupId = "BingTestDemo";
        BingHighLevelConsumer consumer_1 = new BingHighLevelConsumer(groupId, "consumer_1", 3);
        BingHighLevelConsumer consumer_2 = new BingHighLevelConsumer(groupId, "consumer_2", 3);

        consumer_1.consume();
        consumer_2.consume();
    }
}
