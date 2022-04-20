package kafka.firstExemple;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssaineSeek {
    public static void main(String[] args) {

        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssaineSeek.class.getName());

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer <String, String> consumer = new KafkaConsumer<String, String>(properties);
        // assaine
        TopicPartition partitionToreadFrom = new TopicPartition(topic, 0);
        long ofsettoreadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToreadFrom));

        //seek
        consumer.seek(partitionToreadFrom, ofsettoreadFrom);
        int numForRead = 5;
        boolean keepOnReading = true;
        int i =0;

        while (keepOnReading){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record : records ){
                i +=1;

                logger.info("Key: " + record.key() + " Value: " + record.value());
                logger.info("Topic: " + record.topic() + "\n" +
                        "Partition: " + record.partition() + "\n" +
                        "Offsets: " + record.offset());
                if (i<=numForRead){
                    keepOnReading = false;
                    break;
                }

            }
        }
        logger.info("exited from app");
        //subscribe to topics

    }
}
