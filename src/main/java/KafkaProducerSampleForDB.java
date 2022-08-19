import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class KafkaProducerSampleForDB {
    private static final String TOPIC_NAME = "my_topic_test"; // event 보낼 topic 이름


    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");  // broker service 주소
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // 직렬화 설정
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // 직렬화 설정

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties); // 위 설정 토대로 kafka producer 생성

        Scanner sc = new Scanner(System.in);
        System.out.print("Input > ");
        String message = sc.nextLine();     // event에 담을 message 입력

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, message); // topic에 보낼 event 생성
        try {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    // some exception
                }
            });

        } catch (Exception e) {
            // exception
        } finally {
            producer.flush(); // producer 비우기
        }
        producer.close();
    }
}
