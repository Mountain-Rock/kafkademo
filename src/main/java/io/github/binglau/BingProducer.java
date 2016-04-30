package io.github.binglau;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.*;


/**
 * 类Producer.java的实现描述：TODO:类实现描述
 *
 * @author bingjian.lbj 2016-04-30 下午5:39
 */
public class BingProducer {

    public ProducerConfig config(){
        Properties props = new Properties();
        props.put("metadata.broker.list", Config.KAFKA_ADDRESS);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "io.github.binglau.BingPartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        return config;
    }

    public KeyedMessage<String, String> getMsg(int i){
        String key = i + "";
        String msg = "Message: " + i;
        return new KeyedMessage<String, String>(Config.TOPIC, key, msg);
    }

    public void singleModeProduce(ProducerConfig config) throws InterruptedException{
        Producer<String, String> producer = new Producer<String, String>(config);
        for(int i = 0; i < 10000; i++){
            producer.send(getMsg(i));
            System.out.println(i);
            Thread.sleep(1000);
        }
    }

    public void batchModeProduce(ProducerConfig config){
        List<KeyedMessage<String, String>> messages = new ArrayList<KeyedMessage<String, String>>(100);
        Producer<String, String> producer = new Producer<String, String>(config);
        for(int i = 0; i < 1000; i++){
            messages.add(getMsg(i));
            if (i % 100 == 0){
                producer.send(messages);
                messages.clear();
            }
        }
        producer.send(messages);
    }

    public static void main(String[] args) throws InterruptedException{
        BingProducer producer = new BingProducer();
        ProducerConfig config = producer.config();
        producer.batchModeProduce(config);
    }

}
