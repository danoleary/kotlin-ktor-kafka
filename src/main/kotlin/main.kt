import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*

import java.util.Properties

const val WORD_COUNT_STORE = "word-count-store"
private const val bootstrapServers = "localhost:9092"
private val streamsConfiguration = Properties()


fun main(args: Array<String>) {

    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-lambda-example")
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-lambda-example-client")
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000)
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)

    val builder = StreamsBuilder()

    val textLines: KStream<String, String> = builder.stream("streams-plaintext-input")

    val wordCounts: KGroupedStream<String, String> = textLines
            .flatMapValues({value -> value.split("\\W+") })
            .groupBy({ _, word -> word})

    wordCounts.count(Materialized.`as`(WORD_COUNT_STORE))

    val streams = KafkaStreams(builder.build(), streamsConfiguration)

    streams.cleanUp()

    streams.start()

    startServer(streams)

    Runtime.getRuntime().addShutdownHook(Thread(streams::close))
}