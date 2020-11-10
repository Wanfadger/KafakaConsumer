package com.wanfadger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
       final String bootstrapServer="localhost:9092";
       final String consumerGroup = "javaGroup";
       final String offset = "earliest";

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrapServer);//specifies url where kafka broker is running
        /*
        in case of more than one server, specify them while comma separated
        localhost:9092 , localhost:9093 , localhost:9094
         */

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class);//key type and deserializer
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class);//value type and deserializer
        properties.put(ConsumerConfig.GROUP_ID_CONFIG , consumerGroup);//consumer group name
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , offset);//index to start reading
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG , false);
        //tells kafak that will do commiting manually

        KafkaConsumer<String , String> consumer = new KafkaConsumer<String, String>(properties);

        //consuming or polling from a topic
        //before we can poll any record we need to subscribe to one or more topics
        consumer.subscribe(Collections.singletonList("java_topic"));

        //the easiest to poll records is an endless loop
        while (true){
            ConsumerRecords<String ,String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String , String> record : records) {
                String result = "";
                result.concat("key "+record.key())
                        .concat(" value "+record.value())
                        .concat("partition "+record.partition())
                        .concat("topic "+record.topic())
                        .concat("offset "+record.offset());

                System.out.println("Key "+record.key() +" value "+record.value());
            }
            /*
            consumer.poll(Duration) will return immediately if their available records , otherwise it will block
            until either a record is available or timeout expires
             if time expires , poll will return an empty result set
             */

            consumer.commitAsync();
            /*
            committing offset manually , only required after message is processed
             */
        }

    }

}
