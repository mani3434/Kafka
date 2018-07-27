import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;

import java.util.ArrayList;
import java.util.Properties;

public class KafkaConsumerSubscribeApp {

    public static void main(String[] args) {


        Properties props = new Properties();

        props.put("bootstrap.servers","localhost:9092");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id","test");

        KafkaConsumer consumer = new KafkaConsumer(props);

        ArrayList<String> topics = new ArrayList<String>();
        topics.add("c1");
        topics.add("c2");

        consumer.subscribe(topics);

        try {
            while (true){
                ConsumerRecords<String,String> records = consumer.poll(10);
                for (ConsumerRecord<String,String> record:records){
                    System.out.println(
                            String.format("Topic: %s, Partition: %d, offset:%d, key:%s, value:%s",
                                    record.topic(),record.partition(),record.offset(),record.key(),record.value()));

                }
            }
        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            consumer.close();
        }

    }
}
