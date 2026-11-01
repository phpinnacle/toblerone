package org.phpinnacle.toblerone

import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.apache.kafka.connect.transforms.util.SimpleConfig

@Suppress("TooManyFunctions")
abstract class EmptyStringToNullTransform<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        @Suppress("unused")
        const val OVERVIEW_DOC = "Replace empty strings in selected fields with null"

        val CONFIG_DEF: ConfigDef = ConfigDef().define(
            "fields", ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Validator { name, value ->
                val names = value as? List<*>
                    ?: throw ConfigException(name, value, "Must be a nonempty list of field names")
                if (names.isEmpty() || names.any { it !is String || it.isBlank() }) {
                    throw ConfigException(name, value, "Must contain at least one field and no empty names")
                }
            },
            ConfigDef.Importance.HIGH, "Nonempty list of comma separated fields to convert"
        ).define(
            "trim", ConfigDef.Type.BOOLEAN, false,
            ConfigDef.Importance.MEDIUM, "Trim leading and trailing whitespace before checking for empty strings"
        )

        private const val PURPOSE = "toblerone-empty-string-to-null"
        private const val CACHE_SIZE = 16
    }

    private lateinit var fields: Set<String>
    private var trim = false
    private lateinit var schemaCache: SynchronizedCache<Schema, Schema>

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)
        fields = config.getList("fields").map { it.trim() }.toSet()
        trim = config.getBoolean("trim")
        schemaCache = SynchronizedCache(LRUCache(CACHE_SIZE))
    }

    override fun apply(record: R): R = when {
        operatingValue(record) == null -> record
        operatingSchema(record) == null -> applySchemaless(record)
        else -> applyWithSchema(record)
    }

    override fun close() = Unit
    override fun config(): ConfigDef = CONFIG_DEF

    protected abstract fun operatingSchema(record: R?): Schema?
    protected abstract fun operatingValue(record: R?): Any?
    protected abstract fun newRecord(record: R?, schema: Schema?, value: Any?): R

    private fun applySchemaless(record: R): R {
        val value = Requirements.requireMap(operatingValue(record), PURPOSE)
        return newRecord(record, null, value.mapValues { (name, item) -> convert(name, item) })
    }

    private fun applyWithSchema(record: R): R {
        val value = Requirements.requireStruct(operatingValue(record), PURPOSE)
        val schema = value.schema()
        val outputSchema = copySchema(schema)
        val output = Struct(outputSchema)
        for (field in schema.fields()) {
            val item = if (field.schema().isOptional) value.getWithoutDefault(field.name()) else value.get(field)
            output.put(field.name(), convert(field.name(), item))
        }
        return newRecord(record, outputSchema, output)
    }

    private fun convert(field: String, value: Any?): Any? {
        if (field !in fields || value !is String) return value
        val output = if (trim) value.trim() else value
        return output.ifEmpty { null }
    }

    private fun copySchema(schema: Schema): Schema {
        schemaCache.get(schema)?.let { return it }
        val output = SchemaUtil.copySchemaBasics(schema)
        if (schema.isOptional) output.optional()
        for (field in schema.fields()) {
            val fieldSchema = field.schema()
            val outputSchema = if (field.name() in fields && fieldSchema.type() == Schema.Type.STRING) {
                // Defaults would replace the null produced by this transformation.
                SchemaUtil.copySchemaBasics(fieldSchema, SchemaBuilder.string()).optional().build()
            } else {
                fieldSchema
            }
            output.field(field.name(), outputSchema)
        }
        val result = output.build()
        schemaCache.put(schema, result)
        return result
    }

    class Key<R : ConnectRecord<R>?> : EmptyStringToNullTransform<R>() {
        override fun operatingSchema(record: R?): Schema? = record?.keySchema()
        override fun operatingValue(record: R?): Any? = record?.key()
        override fun newRecord(record: R?, schema: Schema?, value: Any?): R = record!!.newRecord(
            record.topic(), record.kafkaPartition(), schema, value,
            record.valueSchema(), record.value(), record.timestamp()
        )
    }

    class Value<R : ConnectRecord<R>?> : EmptyStringToNullTransform<R>() {
        override fun operatingSchema(record: R?): Schema? = record?.valueSchema()
        override fun operatingValue(record: R?): Any? = record?.value()
        override fun newRecord(record: R?, schema: Schema?, value: Any?): R = record!!.newRecord(
            record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
            schema, value, record.timestamp()
        )
    }
}
