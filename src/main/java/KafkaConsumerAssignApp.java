import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Properties;

public class KafkaConsumerAssignApp {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put("bootstrap.servers","localhost:9092");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer myConsumer = new KafkaConsumer(props);

        ArrayList<TopicPartition> partitions = new ArrayList<TopicPartition>();

        TopicPartition c1Part0 = new TopicPartition("c1",0);
        TopicPartition c2Part2 = new TopicPartition("c2",2);

        partitions.add(c1Part0);
        partitions.add(c2Part2);

        myConsumer.assign(partitions);

        try{
            while (true){
                ConsumerRecords<String,String> records = myConsumer.poll(10);
                for (ConsumerRecord<String,String> record:records){
                    System.out.println(
                            String.format("Topic: %s, Partition: %d, offset:%d, key:%s, value:%s",
                                    record.topic(),record.partition(),record.offset(),record.key(),record.value()));

                }
            }
        }catch (Exception e){

        }


    }
}
