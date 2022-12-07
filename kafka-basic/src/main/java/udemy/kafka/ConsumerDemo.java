package udemy.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        log.info("Kafka Consumer"); // slf4j simple: testImplementation -> implementation 으로 동작

        basicConsumer();
    }

    /**
     * basic consumer
     * 1. create consumer configs
     * 2. create consumer
     * 3. subscribe consumer to our topic(s)
     * 4. poll for new data
     */

    public static void basicConsumer() {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-second-application";
        String topic = "demo_java";

        //1. create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        //키,값 역직렬화
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //컨슈머 그룹 설정
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        //offset
        // none : 오프셋이 발견되지 않으면 실행하지 않음
        // earliest : 처음부터 읽음
        // latest : 지금부터 읽음
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        //2. create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


        //3. subscribe consumer to our topic(s)
        // consumer.subscribe(Collections.singleton(topic));
        consumer.subscribe(Arrays.asList(topic));


        //4. poll for new data
        while (true) {
            log.info("Polling");

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // 지속시간: 최대 100ms 기다림

            for (ConsumerRecord<String, String> record: records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }

            // 무한루프
        }

    }

}
