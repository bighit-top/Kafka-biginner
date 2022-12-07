package udemy.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        log.info("Hello world!"); // slf4j simple: testImplementation -> implementation 으로 동작

        basicProducer();
    }

    /**
     * basic producer
     * 1. create producer properties
     * 2. create the producer
     * 3. create a producer record
     * 4. send data - asynchronous
     * 5. flush data - synchronous
     * 6. flush and close the producer
     */
    public static void basicProducer() {

        //1. create producer properties
        Properties properties = new Properties();
        //properties.setProperty("key", "value");

        // 부트스트랩 브로커 정보 : $ kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); //"bootstrap.servers"
        // 키,값 직렬화. 데이터 변환
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //"key.serializer"
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //"value.serializer"


        //2. create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        //3. create a producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "hello world");


        //4. send data - asynchronous
        producer.send(producerRecord);


        //5. flush data - synchronous
        producer.flush();


        //6. flush and close the producer
        producer.close();

    }

}
