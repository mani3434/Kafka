package com.consumerGroup;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class KafkaConsumerGroupApp1 {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put("bootstrap.servers","localhost:9092,localhost:9093");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id","test-group" );


        KafkaConsumer consumer = new KafkaConsumer(props);

        ArrayList<String> topics = new ArrayList<>();
        topics.add("topic-1");

        consumer.subscribe(topics);


        try
        {

            while (true){
                ConsumerRecords<String,String> records = consumer.poll(10);
                for (ConsumerRecord<String,String> record: records){

                    System.out.printf("Topics: %s, Partition: %d , Value: %s",record.topic(), record.partition(), record.value().toUpperCase());
                    System.out.println();
                }
            }
        }catch (Exception e) {
            System.out.println(e.getMessage());
        }finally {
            consumer.close();
        }
    }
}
