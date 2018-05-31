package postApi

import com.google.gson.Gson
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import shared.Command

import java.util.Properties;


fun produce(command: Command) {
    val properties = Properties()

    // kafka bootstrap server
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    // producer acks
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all") // strongest producing guarantee
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3")
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1")
    // leverage idempotent producer from Kafka 0.11 !
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true") // ensure we don't push duplicates

    val producer = KafkaProducer<String, String>(properties)

    val record = ProducerRecord("commands", command.aggregateId, Gson().toJson(command))

    producer.send(record)

    producer.close()
}