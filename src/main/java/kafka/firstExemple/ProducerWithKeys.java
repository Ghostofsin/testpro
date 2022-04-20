package kafka.firstExemple;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);

        String bootstrapServer = "127.0.0.1:9092";
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create producer
        KafkaProducer <String,String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i<10; i++) {
            // create producer record

            String topic = "first_topic";
            String value = "hello shit world " + Integer.toString(i);
            String key = "Key(id)_" + Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic,  key, value);

            logger.info("Key " + key);
            // send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        // if record was successfully sent
                        logger.info("resieved new metadata. \n " +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offsets: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);

                    }
                }
            }).get();
        }
        // flush data
        producer.flush();
        // close producer
        producer.close();
    }
}
