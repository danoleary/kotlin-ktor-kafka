package shared

data class Command(val aggregateId: String, val type: String, val payload: Any)
data class CommandResponse(val accepted: Boolean, val reason: String)

data class CreateUserCommand(val firstName: String, val lastName: String)
data class UpdateUserCommand(val firstName: String, val lastName: String)

fun commandHasValidPayload(command: Command) =
    when (command.type) {
        "CREATE_USER" -> command.payload is CreateUserCommand
        "UPDATE_USER" -> command.payload is UpdateUserCommand
        else -> false
    }
