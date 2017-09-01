package com.lwc;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * kafka消费者
 * Created by Corn on 2017/4/5.
 */
public class kafkaConsumer extends Thread {
    private static String topic;
    private static String zookeeper;
    private static String groupid;
    public kafkaConsumer(String topic) {
        Properties conf = new Properties();
        try {
            conf.load(KafkaProducerTest.class.getClass().getResourceAsStream("/params.properties"));
            this.zookeeper=conf.getProperty("zookeeper.servers");
            this.topic = conf.getProperty("kafka.topic");
            this.groupid=conf.getProperty("kafka.groupid");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Override
    public void run() {
        ConsumerConnector consumer = createConsumer();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1); // 一次从主题中获取一个数据
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        while (iterator.hasNext()) {
            String message = new String(iterator.next().message());
            System.out.println("接收到: " + message);
        }
    }

    private ConsumerConnector createConsumer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", zookeeper);//声明zk
        properties.put("group.id", groupid);// 必须要使用别的组名称， 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }


    public static void main(String[] args) {
        new kafkaConsumer(topic).start();// 使用kafka集群中创建好的主题 test

    }
}
