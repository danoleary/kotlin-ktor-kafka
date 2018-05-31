package domain

import com.google.gson.Gson
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import shared.CommandResponse
import java.util.*

private const val bootstrapServers = "localhost:9092"
private val streamsConfiguration = Properties()

fun main(args: Array<String>) {

    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-cqrs-domain")
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "kafka-streams-cqrs-domain")
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000)
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)

    val builder = StreamsBuilder()

    val commands: KStream<String, String> =
            builder.stream("commands")

    commands
            .mapValues { value -> Gson().toJson(CommandResponse(Random().nextBoolean(), "the type is: $value")) }
            .to("command-responses")

    val streams = KafkaStreams(builder.build(), streamsConfiguration)

    streams.cleanUp()

    streams.start()

    Runtime.getRuntime().addShutdownHook(Thread(streams::close))
}