package commandLogApi

import io.ktor.application.*
import io.ktor.features.ContentNegotiation
import io.ktor.gson.gson
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import java.lang.Thread.sleep
import java.text.DateFormat

fun startServer(streams: KafkaStreams) {
    val server = embeddedServer(Netty, port = 8081) {
        install(ContentNegotiation) {
            gson {
                setDateFormat(DateFormat.LONG)
                setPrettyPrinting()
            }
        }
        routing {
            get("/") {
                call.respondText("I'm up!", ContentType.Text.Plain)
            }
            get("/commands") {
                val id = call.request.queryParameters["id"]
                val commands: ReadOnlyKeyValueStore<String, String> =
                        streams.store("command-log-2", QueryableStoreTypes.keyValueStore())
                sleep(1000)
                call.respondText("commands: " + commands.get(id))
            }
        }
    }
    server.start(wait = true)
}