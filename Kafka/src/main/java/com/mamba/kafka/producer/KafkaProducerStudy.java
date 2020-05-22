package com.kaikeba.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 需求：开发kafka生产者代码
 */
public class KafkaProducerStudy {
    public static void main(String[] args) {
        //准备配置属性
        Properties props = new Properties();
        //kafka集群地址
        props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        //acks它代表消息确认机制
        props.put("acks", "all");
        //重试的次数
        props.put("retries", 0);
        //批处理数据的大小，每次写入多少数据到topic
        props.put("batch.size", 16384);
        //可以延长多久发送数据
        props.put("linger.ms", 1);
        //缓冲区的大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++) {
            //这里需要三个参数，第一个：topic的名称，第二个参数：表示消息的key,第三个参数：消息具体内容
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), "hello-kafka-" + i));
        }
        producer.close();
    }

}
