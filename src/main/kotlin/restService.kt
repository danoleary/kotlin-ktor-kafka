import io.ktor.application.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore

fun startServer(streams: KafkaStreams) {
    val server = embeddedServer(Netty, port = 8080) {
        routing {
            get("/") {
                call.respondText("Hello World!", ContentType.Text.Plain)
            }
            get("/word") {
                val word = call.request.queryParameters["word"]
                val wordCounts: ReadOnlyKeyValueStore<String, Long> = streams.store(WORD_COUNT_STORE, QueryableStoreTypes.keyValueStore())
                call.respondText("word count: " + wordCounts.get(word))
            }
        }
    }
    server.start(wait = true)
}