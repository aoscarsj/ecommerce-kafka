package ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
                                        //Key,   value
        var producer = new KafkaProducer<String, String>(properties());

        var value = "001,015,6990";
                                                        //topic
        var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", value, value);
//        producer.send(record); // this method is assync, to wait use .get
        producer.send(record, (data, ex) -> { //override, callback, data -> sucess ex -> exception
            if(ex != null){
                ex.printStackTrace();
                return;
            }
            System.out.println("Send successfully: " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        }).get();
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //Serialize strings into bytes
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
