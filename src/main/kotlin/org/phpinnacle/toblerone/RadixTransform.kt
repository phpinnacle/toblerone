package org.phpinnacle.toblerone

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
    }

    private lateinit var fields: Map<String, Int>

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)

        fields = config.getList("fields").map { it.trim() }.associateBy(
            { key -> key.split(':').first() },
            { value -> (value.split(':').drop(1).lastOrNull()?.toInt()) ?: 0 }
        ).filter { it.value != 0 }
    }

    override fun apply(record: R): R {
        return when {
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
    }

    @Suppress("EmptyFunctionBlock")
    override fun close() {
    }

    override fun config(): ConfigDef {
        return CONFIG_DEF
    }

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

        val outputValues = mutableMapOf<String, Any>()
        val outputSchema = SchemaUtil.copySchemaBasics(schema)

        for (field in schema.fields()) {
            val name = field.name()

            if (name !in fields.keys) {
                outputValues[name] = value.get(field)
                outputSchema.field(name, field.schema())

                continue
            }

            val (newSchema, newValue) = convert(name, field.schema(), value)

            outputSchema.field(name, newSchema)
            outputValues[name] = newValue
        }

        val outputStruct = Struct(outputSchema)

        outputValues.forEach { outputStruct.put(it.key, it.value) }

        return newRecord(record, outputSchema.schema(), outputStruct)
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

    private fun convert(key: String, schema: Schema, value: Struct): Pair<Schema, Any> {
        val radix = fields[key] ?: return Pair(schema, value)

        return when (schema.type()) {
            Schema.Type.INT8 -> Pair(Schema.STRING_SCHEMA, value.getInt16(key).toString(radix))
            Schema.Type.INT16 -> Pair(Schema.STRING_SCHEMA, value.getInt16(key).toString(radix))
            Schema.Type.INT32 -> Pair(Schema.STRING_SCHEMA, value.getInt32(key).toString(radix))
            Schema.Type.INT64 -> Pair(Schema.STRING_SCHEMA, value.getInt64(key).toString(radix))
            Schema.Type.STRING -> Pair(Schema.INT32_SCHEMA, value.getString(key).trim().toInt(radix))
            else -> Pair(schema, value)
        }
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
