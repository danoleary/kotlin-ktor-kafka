package shared

data class Command(val aggregateId: String, val type: String)
data class CommandResponse(val accepted: Boolean, val reason: String)