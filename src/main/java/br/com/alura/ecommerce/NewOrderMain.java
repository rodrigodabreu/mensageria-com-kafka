package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(final String[] args) throws ExecutionException, InterruptedException {
            KafkaProducer producer = new KafkaProducer<String, String>(properties());
            String value = "1234, 22222, 33344557788";
        ProducerRecord record = new ProducerRecord<String, String>("ecommerce_new_order", value, value);

        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }

            System.out.println("sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/offset " + data.offset() + "/timestamp " + data
                    .timestamp());
        };

        String email = "Thank you for your order! We are processing your order";
        ProducerRecord emailRecord = new ProducerRecord("ecommerce_send_email", email, email);
        producer.send(record, callback).get();
        producer.send(emailRecord,callback).get();
    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;

    }
}
