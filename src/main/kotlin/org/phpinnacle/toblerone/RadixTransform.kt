package org.phpinnacle.toblerone

import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.apache.kafka.connect.transforms.util.SimpleConfig

@Suppress("TooManyFunctions")
abstract class RadixTransform<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        val OVERVIEW_DOC = (
            "Convert fields or the entire key or value to a specific radix, " +
            "e.g. to force an base10 integer field to a base36 string. " +
            "Convert integer fields to strings and string fields to integers. " +
            "Use the concrete transformation type designed for " +
            "the record key (<code>" + Key::class.java.getName() + "</code>) or" +
            "the record value (<code>" + Value::class.java.getName() + "</code>)."
        )

        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(
                "fields",
                ConfigDef.Type.LIST,
                ConfigDef.Importance.HIGH,
                "List of comma separated fields to convert"
            )

        private const val PURPOSE = "base-convert-transform"

        private val cache = SynchronizedCache(LRUCache<Schema, Schema>(16))
    }

    private lateinit var fields: Map<String, Int>

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)

        fields = config.getList("fields").map { it.trim() }.associateBy(
            { key -> key.split(':').first() },
            { value -> (value.split(':').drop(1).lastOrNull()?.toInt()) ?: 0 }
        ).filter { it.value != 0 }
    }

    override fun apply(record: R): R = when {
        operatingValue(record) == null -> {
            record
        }
        operatingSchema(record) == null -> {
            applySchemaless(record)
        }
        else -> {
            applyWithSchema(record)
        }
    }

    @Suppress("EmptyFunctionBlock")
    override fun close() {
    }

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
        val outputValues = Struct(outputSchema)

        for (field in schema.fields()) {
            val name = field.name()

            if (name in fields.keys) {
                outputValues.put(name, convert(name, field.schema(), value))
            } else {
                outputValues.put(name, value.get(field))
            }
        }

        return newRecord(record, outputSchema.schema(), outputValues)
    }

    private fun convert(key: String, value: Any): Any {
        val radix = fields[key] ?: return value

        return when (value) {
            is String -> value.trim().toInt(radix)
            is Int -> value.toString(radix)
            is Long -> value.toString(radix)
            is Short -> value.toString(radix)
            else -> value
        }
    }

    private fun convert(key: String, schema: Schema, value: Struct): Any {
        val radix = fields[key] ?: return value

        return when (schema.type()) {
            Schema.Type.INT8 -> value.getInt16(key).toString(radix)
            Schema.Type.INT16 -> value.getInt16(key).toString(radix)
            Schema.Type.INT32 -> value.getInt32(key).toString(radix)
            Schema.Type.INT64 -> value.getInt64(key).toString(radix)
            Schema.Type.STRING -> value.getString(key).trim().toInt(radix)
            else -> value
        }
    }

    private fun infer(schema: Schema): Schema {
        return when (schema.type()) {
            Schema.Type.INT8 -> Schema.STRING_SCHEMA
            Schema.Type.INT16 -> Schema.STRING_SCHEMA
            Schema.Type.INT32 -> Schema.STRING_SCHEMA
            Schema.Type.INT64 -> Schema.STRING_SCHEMA
            Schema.Type.STRING -> Schema.INT32_SCHEMA
            else -> schema
        }
    }

    private fun copySchema(schema: Schema): Schema
    {
        val cached = cache.get(schema)

        if (cached != null) {
            return cached
        }

        val output = SchemaUtil.copySchemaBasics(schema)

        for (field in schema.fields()) {
            val name = field.name()

            if (name in fields.keys) {
                output.field(name, infer(field.schema()))
            } else {
                output.field(name, field.schema())
            }
        }

        cache.put(schema, output)

        return output
    }

    class Key<R : ConnectRecord<R>?> : RadixTransform<R>() {
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

    class Value<R : ConnectRecord<R>?> : RadixTransform<R>() {
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
