package com.lwc;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Hello world!
 *
 */
public class KafkaProducerTest
{

    public static void main(String[] args) throws Exception {
        Properties conf =new Properties();
        conf.load(KafkaProducerTest.class.getClass().getResourceAsStream("/params.properties"));
        Properties props = new Properties();
        props.put("bootstrap.servers", conf.getProperty("kafka.servers"));
        //The "all" setting we have specified will result in blocking on the full commit of the record, the slowest but most durable setting.
        //“所有”设置将导致记录的完整提交阻塞，最慢的，但最持久的设置。
        props.put("acks", "all");
        //如果请求失败，生产者也会自动重试，即使设置成0 the producer can automatically retry.
        props.put("retries", 0);
        //The producer maintains buffers of unsent records for each partition.
        props.put("batch.size", 16384);
        //默认立即发送，这里这是延时毫秒数
        props.put("linger.ms", 1);
        //生产者缓冲大小，当缓冲区耗尽后，额外的发送调用将被阻塞。时间超过max.block.ms将抛出TimeoutException
        props.put("buffer.memory", 33554432);
        //The key.serializer and value.serializer instruct how to turn the key and value objects the user provides with their ProducerRecord into bytes.
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //创建kafka的生产者类
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        //生产者的主要方法

        for(int i = 0; i < Integer.parseInt(args[0]); i++) {
            String json = KafkaProducerTest.getJson();
            json.replaceAll("\\[card\\]", i+"");
            producer.send(new ProducerRecord(conf.getProperty("kafka.topic"), json));
        }
        producer.close();
        System.out.println("发送成功");
    }

    public static String getJson() {
        try {
            BufferedReader br=new BufferedReader(new InputStreamReader(KafkaProducerTest.class.getClass().getResourceAsStream("/json.txt")));
            StringBuffer sb=new StringBuffer();
            String line=null;
            while ((line=br.readLine())!=null){
                sb.append(line);
            }
            br.close();
            return  sb.toString();
        }
        catch (Exception e){
            return "";
        }

    }
}
