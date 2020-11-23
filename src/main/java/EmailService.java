import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {

    public static void main(String[] args) {


        KafkaConsumer consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Collections.singletonList("ecommerce_send_email")); //um consumer para cada tópico, se um comsumer escutar de vários cria uma bagunca
        while (true) {
            ConsumerRecords records = consumer.poll(Duration.ofMillis(100));
            if (records.isEmpty()) {
                System.out.println("Founded "+ records.count() + " registers");
                continue;
            }

            System.out.println("--------------------------------------------------------");
            System.out.println("Sending email");

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Email sended");
        }
    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName());
        return properties;
    }
}
