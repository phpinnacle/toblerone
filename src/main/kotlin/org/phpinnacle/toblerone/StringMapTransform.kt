package org.phpinnacle.toblerone

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SimpleConfig

@Suppress("TooManyFunctions")
abstract class StringMapTransform<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        @Suppress("unused")
        const val OVERVIEW_DOC = "Replace strings in a field using mapping.<source> settings"

        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(
                "field",
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.CompositeValidator.of(ConfigDef.NonNullValidator(), ConfigDef.NonEmptyString()),
                ConfigDef.Importance.HIGH,
                "Field to transform; configure replacements using mapping.<source> string settings"
            )

        private const val MAPPING_PREFIX = "mapping."
        private const val PURPOSE = "toblerone-string-map"
    }

    private lateinit var field: String
    private lateinit var mapping: Map<String, String>

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)

        field = config.getString("field")
        mapping = config.originals()
            .filterKeys { it.startsWith(MAPPING_PREFIX) }
            .mapKeys { it.key.removePrefix(MAPPING_PREFIX) }
            .mapValues { (source, target) ->
                target as? String ?: throw ConfigException("$MAPPING_PREFIX$source", target, "Must be a string")
            }
            .ifEmpty { throw ConfigException("At least one mapping.<source> setting is required") }
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
        val value = Requirements.requireMap(operatingValue(record), PURPOSE).toMutableMap()

        if (value.containsKey(field)) {
            value[field] = convert(value[field])
        }

        return newRecord(record, null, value)
    }

    private fun applyWithSchema(record: R): R {
        val value = Requirements.requireStruct(operatingValue(record), PURPOSE)
        val schema = operatingSchema(record) ?: return record
        val output = Struct(schema)

        for (schemaField in schema.fields()) {
            val current = value.get(schemaField)
            output.put(schemaField, if (schemaField.name() == field) convert(current) else current)
        }

        return newRecord(record, schema, output)
    }

    private fun convert(value: Any?): Any? = if (value is String) mapping[value] ?: value else value

    class Key<R : ConnectRecord<R>?> : StringMapTransform<R>() {
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

    class Value<R : ConnectRecord<R>?> : StringMapTransform<R>() {
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
