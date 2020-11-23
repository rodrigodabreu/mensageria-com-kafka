import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class FraudDetectorService {

    public static void main(String[] args) {


        KafkaConsumer consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Collections.singletonList("ecommerce_new_order")); //um consumer para cada tópico, se um comsumer escutar de vários cria uma bagunca
        while (true) {
            ConsumerRecords records = consumer.poll(Duration.ofMillis(100));
            if (records.isEmpty()) {
                System.out.println("Founded "+ records.count() + " registers");
                continue;
            }

            System.out.println("--------------------------------------------------------");
            System.out.println("Processing new order, checking for fraud");

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Order processed");
        }
    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());

        /*
        * configurando para consumir de uma em uma msg por vez, com isso se tem um ganho em relação ao commit para não
        * se ter problema em comitar algo que esteja sendo rebalanceado em caso de termos muitas mensagens para
        * serem consumidas
        * Assim temos chance menores de conflitos no rebalanciamento
        * */
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"1");
        return properties;
    }
}
