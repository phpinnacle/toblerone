package org.phpinnacle.toblerone

import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.apache.kafka.connect.transforms.util.SimpleConfig

@Suppress("TooManyFunctions")
abstract class JsonTransform<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        @Suppress("unused")
        const val OVERVIEW_DOC = "Serialize selected fields or the entire key or value to JSON strings"

        val CONFIG_DEF: ConfigDef = ConfigDef().define(
            "fields",
            ConfigDef.Type.LIST,
            emptyList<String>(),
            ConfigDef.Importance.HIGH,
            "Fields to serialize; an empty list serializes the entire key or value"
        )

        private const val PURPOSE = "toblerone-json"
        private const val CACHE_SIZE = 16
    }

    private val converter = JsonConverter().apply {
        configure(mapOf("schemas.enable" to false), false)
    }
    private lateinit var fields: Set<String>
    private lateinit var schemaCache: SynchronizedCache<Schema, Schema>

    override fun configure(props: Map<String?, *>?) {
        fields = SimpleConfig(CONFIG_DEF, props).getList("fields").toSet()
        schemaCache = SynchronizedCache(LRUCache(CACHE_SIZE))
    }

    override fun apply(record: R): R {
        val value = operatingValue(record) ?: return record
        val schema = operatingSchema(record)

        return when {
            fields.isEmpty() -> newRecord(record, schema?.let { stringSchema(it) }, convert(schema, value))
            schema == null -> applySchemaless(record)
            else -> applyWithSchema(record, schema)
        }
    }

    override fun close() = converter.close()
    override fun config(): ConfigDef = CONFIG_DEF

    protected abstract fun operatingSchema(record: R?): Schema?
    protected abstract fun operatingValue(record: R?): Any?
    protected abstract fun newRecord(record: R?, schema: Schema?, value: Any?): R

    private fun applySchemaless(record: R): R {
        val value = Requirements.requireMap(operatingValue(record), PURPOSE)
        val output = value.mapValues { (name, item) -> if (name in fields) convert(null, item) else item }
        return newRecord(record, null, output)
    }

    private fun applyWithSchema(record: R, schema: Schema): R {
        val value = Requirements.requireStruct(operatingValue(record), PURPOSE)
        val outputSchema = copySchema(schema)
        val output = Struct(outputSchema)

        for (field in schema.fields()) {
            val item = value.get(field)
            output.put(field.name(), if (field.name() in fields) convert(field.schema(), item) else item)
        }

        return newRecord(record, outputSchema, output)
    }

    private fun convert(schema: Schema?, value: Any?): String? =
        value?.let { converter.fromConnectData("", schema, it).toString(Charsets.UTF_8) }

    private fun stringSchema(schema: Schema): Schema {
        val output = SchemaBuilder.string()
        if (schema.isOptional) output.optional()
        schema.defaultValue()?.let { output.defaultValue(convert(schema, it)) }
        return output.build()
    }

    private fun copySchema(schema: Schema): Schema {
        val cached = schemaCache.get(schema)
        if (cached != null) return cached

        val output = SchemaUtil.copySchemaBasics(schema)
        if (schema.isOptional) output.optional()

        for (field in schema.fields()) {
            output.field(field.name(), if (field.name() in fields) stringSchema(field.schema()) else field.schema())
        }

        val result = output.build()
        schemaCache.put(schema, result)
        return result
    }

    class Key<R : ConnectRecord<R>?> : JsonTransform<R>() {
        override fun operatingSchema(record: R?): Schema? = record?.keySchema()
        override fun operatingValue(record: R?): Any? = record?.key()
        override fun newRecord(record: R?, schema: Schema?, value: Any?): R = record!!.newRecord(
            record.topic(),
            record.kafkaPartition(),
            schema,
            value,
            record.valueSchema(),
            record.value(),
            record.timestamp()
        )
    }

    class Value<R : ConnectRecord<R>?> : JsonTransform<R>() {
        override fun operatingSchema(record: R?): Schema? = record?.valueSchema()
        override fun operatingValue(record: R?): Any? = record?.value()
        override fun newRecord(record: R?, schema: Schema?, value: Any?): R = record!!.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            schema,
            value,
            record.timestamp()
        )
    }
}
