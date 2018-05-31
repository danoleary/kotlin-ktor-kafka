package postApi

import io.ktor.application.*
import io.ktor.features.ContentNegotiation
import io.ktor.gson.gson
import io.ktor.http.*
import io.ktor.request.receive
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import shared.Command
import java.lang.Thread.sleep
import java.text.DateFormat

fun startServer(streams: KafkaStreams) {
    val server = embeddedServer(Netty, port = 8080) {
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
            post("/") {
                val command = call.receive<Command>()
                println("type = $command.type, aggregate id = ${command.aggregateId}")
                produce(command)
                val commandResponses: ReadOnlyKeyValueStore<String, String> =
                        streams.store("command-response-store", QueryableStoreTypes.keyValueStore())
                sleep(1000)
                call.respondText("command response: " + commandResponses.get(command.aggregateId))
            }
        }
    }
    server.start(wait = true)
}