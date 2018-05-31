package commandLogApi

import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import shared.Command

import java.util.Properties

private const val bootstrapServers = "localhost:9092"
private val streamsConfiguration = Properties()

fun main(args: Array<String>) {

    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "command-log-api")
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "command-log-api")
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000)
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)

    val builder = StreamsBuilder()

    val commands: KGroupedStream<String, String> =
            builder.stream<String, String>("commands").groupBy({ key, _ -> key })

    commands.aggregate(
            { "[]" },
            {key: String, newVal: String, agg: String -> reduceCommands(agg, newVal)},
            Serdes.String(),
            "command-log-2")

    val streams = KafkaStreams(builder.build(), streamsConfiguration)

    streams.cleanUp()

    streams.start()

    startServer(streams)

    Runtime.getRuntime().addShutdownHook(Thread(streams::close))
}

fun reduceCommands(existingCommandsJson: String, newCommandJson: String): String {
    val gsonBuilder = GsonBuilder().serializeNulls()
    val gson = gsonBuilder.create()

    val existingCommands = gson.fromJson(existingCommandsJson, Array<Command>::class.java)
    val newCommand = gson.fromJson(newCommandJson, Command::class.java)

    return gson.toJson(existingCommands.plus(newCommand))
}