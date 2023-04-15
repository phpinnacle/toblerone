package org.phpinnacle.toblerone

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SimpleConfig

abstract class TrimTransform<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        val OVERVIEW_DOC = (
            "Extracts parts of string fields or the entire key or value." +
            "Use the concrete transformation type designed for " +
            "the record key (<code>" + RadixTransform.Key::class.java.getName() + "</code>) or " +
            "the record value (<code>" + RadixTransform.Value::class.java.getName() + "</code>)."
        )

        val CONFIG_DEF: ConfigDef = ConfigDef().define(
            "fields",
            ConfigDef.Type.LIST,
            ConfigDef.Importance.HIGH,
            "List of comma separated fields to trim"
        )

        private const val PURPOSE = "trim-transform"
    }

    private lateinit var fields: List<String>

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)

        fields = config.getList("fields").map { it.trim() }
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
    protected abstract fun newRecord(record: R?, updatedValue: Any?): R

    private fun applySchemaless(record: R): R {
        val value = Requirements.requireMap(operatingValue(record), PURPOSE)
        val output = value.mapValues {
            if (it.key in fields && it.value is String) (it.value as String).trim() else it.value
        }

        return newRecord(record, output)
    }

    private fun applyWithSchema(record: R): R {
        val value = Requirements.requireStruct(operatingValue(record), PURPOSE)
        val schema = operatingSchema(record) ?: return record

        val output = Struct(schema)

        for (field in schema.fields()) {
            val item = value.get(field)

            if (field.name() in fields && item is String) {
                output.put(field, item.trim())
            } else {
                output.put(field, item)
            }
        }

        return newRecord(record, output)
    }

    class Key<R : ConnectRecord<R>?> : TrimTransform<R>() {
        override fun operatingSchema(record: R?): Schema? = record?.keySchema()

        override fun operatingValue(record: R?): Any? = record?.key()

        override fun newRecord(record: R?, updatedValue: Any?): R = record!!.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            updatedValue,
            record.valueSchema(),
            record.value(),
            record.timestamp()
        )
    }

    class Value<R : ConnectRecord<R>?> : TrimTransform<R>() {
        override fun operatingSchema(record: R?): Schema? = record?.valueSchema()

        override fun operatingValue(record: R?): Any? = record?.value()

        override fun newRecord(record: R?, updatedValue: Any?): R = record!!.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            record.valueSchema(),
            updatedValue,
            record.timestamp()
        )
    }
}
