package org.phpinnacle.toblerone

import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.apache.kafka.connect.transforms.util.SimpleConfig
import java.time.Instant
import java.util.Date

@Suppress("TooManyFunctions")
class DebeziumTimestampsTransform<R : ConnectRecord<R>> : Transformation<R> {
    companion object {
        @Suppress("unused")
        const val OVERVIEW_DOC = "Fill missing creation and update timestamps in Debezium change events"

        val CONFIG_DEF = ConfigDef()
            .define(
                "created.field", ConfigDef.Type.STRING, "created_at",
                ConfigDef.CompositeValidator.of(ConfigDef.NonNullValidator(), ConfigDef.NonEmptyString()),
                ConfigDef.Importance.HIGH, "Creation timestamp field in the after row"
            )
            .define(
                "updated.field", ConfigDef.Type.STRING, "updated_at",
                ConfigDef.CompositeValidator.of(ConfigDef.NonNullValidator(), ConfigDef.NonEmptyString()),
                ConfigDef.Importance.HIGH, "Update timestamp field in the after row"
            )
        private const val CACHE_SIZE = 16
        private const val MICROS_PER_MILLI = 1_000L
        private const val PURPOSE = "toblerone-debezium-timestamps"
        private val TIMESTAMP_SCHEMA = Timestamp.builder().optional().build()
    }

    private lateinit var createdField: String
    private lateinit var fields: List<String>
    private var cache = SynchronizedCache(LRUCache<Schema, Schema>(CACHE_SIZE))

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)
        createdField = config.getString("created.field")
        val updatedField = config.getString("updated.field")
        if (createdField == updatedField) {
            throw ConfigException("updated.field", updatedField, "Must differ from created.field")
        }
        fields = listOf(createdField, updatedField)
        cache = SynchronizedCache(LRUCache(CACHE_SIZE))
    }

    override fun config(): ConfigDef = CONFIG_DEF
    override fun close() = Unit

    override fun apply(record: R): R {
        if (record.value() == null) return record
        return if (record.valueSchema() == null) applySchemaless(record) else applyWithSchema(record)
    }

    private fun applyWithSchema(record: R): R {
        val envelope = Requirements.requireStruct(record.value(), PURPOSE)
        val schema = envelope.schema()
        val op = schema.field("op")?.let { envelope.get(it) }
        val after = schema.field("after")?.let { envelope.get(it) }
        if (op !in listOf("c", "r", "u") || after == null) return record
        val row = Requirements.requireStruct(after, PURPOSE)
        val outputSchema = copySchema(schema)
        val timestamp by lazy { eventTime(schema.field("ts_ms")?.let { envelope.get(it) }) }
        val output = Struct(outputSchema)

        for (field in schema.fields()) {
            val value = envelope.getWithoutDefault(field.name())
            val target = outputSchema.field(field.name()).schema()
            val transformed = when {
                field.name() == "after" -> copyRow(row, target, op == "u") { timestamp }
                field.name() == "before" && value is Struct && target != field.schema() ->
                    copyRow(value, target)
                else -> value
            }
            output.put(field.name(), transformed)
        }

        return newRecord(record, outputSchema, output)
    }

    private fun applySchemaless(record: R): R {
        val envelope = Requirements.requireMap(record.value(), PURPOSE)
        val op = envelope["op"]
        val after = envelope["after"]
        if (op !in listOf("c", "r", "u") || after == null) return record
        val row = Requirements.requireMap(after, PURPOSE).toMutableMap()
        val timestamp by lazy { eventTime(envelope["ts_ms"]) }
        for (field in fields) {
            if (row[field] == null && (field != createdField || op != "u")) row[field] = timestamp
        }
        return newRecord(record, null, envelope + ("after" to row))
    }

    private fun copySchema(schema: Schema): Schema {
        cache.get(schema)?.let { return it }
        val row = schema.field("after").schema()
        val rowBuilder = SchemaUtil.copySchemaBasics(row)
        if (row.isOptional) rowBuilder.optional()
        row.fields().forEach { rowBuilder.field(it.name(), it.schema()) }
        fields.filter { row.field(it) == null }.forEach { rowBuilder.field(it, TIMESTAMP_SCHEMA) }
        val rowSchema = rowBuilder.build()

        val output = SchemaUtil.copySchemaBasics(schema)
        if (schema.isOptional) output.optional()
        for (field in schema.fields()) {
            val replacement = if (field.name() in listOf("before", "after") && field.schema() == row) {
                rowSchema
            } else {
                field.schema()
            }
            output.field(field.name(), replacement)
        }
        val result = output.build()
        cache.put(schema, result)
        return result
    }

    private fun copyRow(
        row: Struct,
        schema: Schema,
        update: Boolean = false,
        timestamp: (() -> Long)? = null
    ): Struct {
        val output = Struct(schema)
        for (field in schema.fields()) {
            val value = row.schema().field(field.name())?.let { row.getWithoutDefault(it.name()) }
            val fill = field.name() in fields && (field.name() != createdField || !update)
            output.put(field, if (value == null && fill && timestamp != null) {
                timestampValue(field.schema(), timestamp())
            } else {
                value
            })
        }
        return output
    }

    private fun timestampValue(schema: Schema, millis: Long): Any = when (schema.name()) {
        Timestamp.LOGICAL_NAME -> Date(millis)
        "io.debezium.time.Timestamp" -> millis
        "io.debezium.time.MicroTimestamp" -> Math.multiplyExact(millis, MICROS_PER_MILLI)
        "io.debezium.time.ZonedTimestamp" -> Instant.ofEpochMilli(millis).toString()
        else -> throw DataException("$PURPOSE cannot fill timestamp field with schema $schema")
    }

    private fun eventTime(value: Any?): Long = when (value) {
        null -> System.currentTimeMillis()
        is Long -> value
        else -> throw DataException("$PURPOSE requires ts_ms to be an INT64 timestamp")
    }

    private fun newRecord(record: R, schema: Schema?, value: Any?): R = record.newRecord(
        record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), schema, value, record.timestamp()
    )
}
