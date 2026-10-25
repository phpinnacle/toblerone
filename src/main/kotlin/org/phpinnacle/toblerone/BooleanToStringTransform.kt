package org.phpinnacle.toblerone

import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.apache.kafka.connect.transforms.util.SimpleConfig

@Suppress("TooManyFunctions")
abstract class BooleanToStringTransform<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        @Suppress("unused")
        const val OVERVIEW_DOC = "Convert selected boolean fields to configured strings"

        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(
                "fields",
                ConfigDef.Type.LIST,
                listOf<String>(),
                ConfigDef.Importance.HIGH,
                "List of comma separated boolean fields to convert"
            )
            .define(
                "true",
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.NonNullValidator(),
                ConfigDef.Importance.HIGH,
                "String to use for true"
            )
            .define(
                "false",
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.NonNullValidator(),
                ConfigDef.Importance.HIGH,
                "String to use for false"
            )

        private const val PURPOSE = "toblerone-boolean-to-string"
        private const val CACHE_SIZE = 16
    }

    private lateinit var fields: Set<String>
    private lateinit var trueValue: String
    private lateinit var falseValue: String
    private lateinit var schemaCache: SynchronizedCache<Schema, Schema>

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)

        fields = config.getList("fields").toSet()
        trueValue = config.getString("true")
        falseValue = config.getString("false")
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
        val output = value.mapValues { convert(it.key, it.value) }

        return newRecord(record, null, output)
    }

    private fun applyWithSchema(record: R): R {
        val value = Requirements.requireStruct(operatingValue(record), PURPOSE)
        val schema = operatingSchema(record) ?: return record
        val outputSchema = copySchema(schema)
        val output = Struct(outputSchema)

        for (field in schema.fields()) {
            output.put(field.name(), convert(field.name(), value.get(field)))
        }

        return newRecord(record, outputSchema, output)
    }

    private fun convert(field: String, value: Any?): Any? =
        if (field in fields && value is Boolean) {
            if (value) trueValue else falseValue
        } else {
            value
        }

    private fun copySchema(schema: Schema): Schema {
        val cached = schemaCache.get(schema)
        if (cached != null) return cached

        val output = SchemaUtil.copySchemaBasics(schema)
        if (schema.isOptional) output.optional()

        for (field in schema.fields()) {
            val fieldSchema = field.schema()
            val outputSchema = if (field.name() in fields && fieldSchema.type() == Schema.Type.BOOLEAN) {
                val builder = SchemaUtil.copySchemaBasics(fieldSchema, SchemaBuilder.string())
                if (fieldSchema.isOptional) builder.optional()
                fieldSchema.defaultValue()?.let { builder.defaultValue(convert(field.name(), it)) }
                builder.build()
            } else {
                fieldSchema
            }
            output.field(field.name(), outputSchema)
        }

        val result = output.build()
        schemaCache.put(schema, result)
        return result
    }

    class Key<R : ConnectRecord<R>?> : BooleanToStringTransform<R>() {
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

    class Value<R : ConnectRecord<R>?> : BooleanToStringTransform<R>() {
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
