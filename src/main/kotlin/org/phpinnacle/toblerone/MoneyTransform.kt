package org.phpinnacle.toblerone

import com.ibm.icu.util.Currency
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.apache.kafka.connect.transforms.util.SimpleConfig
import java.math.BigDecimal

@Suppress("TooManyFunctions")
abstract class MoneyTransform<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        @Suppress("unused")
        const val OVERVIEW_DOC = "Convert selected Decimal or FLOAT64 fields to integer currency minor units"

        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(
                "mode", ConfigDef.Type.STRING, "minor", ConfigDef.ValidString.`in`("minor"),
                ConfigDef.Importance.HIGH, "Conversion mode; currently only minor is supported"
            )
            .define(
                "fields", ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Validator { name, value ->
                    val names = value as? List<*>
                        ?: throw ConfigException(name, value, "Must be a nonempty list of field names")
                    if (names.isEmpty() || names.any { it !is String || it.isBlank() }) {
                        throw ConfigException(name, value, "Must contain at least one field and no empty names")
                    }
                },
                ConfigDef.Importance.HIGH, "Nonempty list of comma separated Decimal or FLOAT64 fields to convert"
            )
            .define(
                "currency", ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.NonNullValidator(),
                ConfigDef.Importance.HIGH, "Three uppercase letter ISO currency code, e.g. RUB"
            )

        private const val PURPOSE = "toblerone-money"
        private const val CACHE_SIZE = 16
    }

    private lateinit var fields: Set<String>
    private var fractionDigits = 0
    private lateinit var schemaCache: SynchronizedCache<Schema, Schema>

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)
        fields = config.getList("fields").map { it.trim() }.toSet()
        val currency = config.getString("currency")
        if (!Regex("[A-Z]{3}").matches(currency) ||
            Currency.getAvailableCurrencies().none { it.currencyCode == currency }
        ) {
            throw ConfigException("currency", currency, "Must be a supported three uppercase letter ISO currency code")
        }
        fractionDigits = Currency.getInstance(currency).defaultFractionDigits
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
        val outputSchema = copySchema(value.schema())
        val output = Struct(outputSchema)
        for (field in value.schema().fields()) {
            val item = if (field.schema().isOptional) value.getWithoutDefault(field.name()) else value.get(field)
            output.put(field.name(), convert(field.name(), item))
        }
        return newRecord(record, outputSchema, output)
    }

    private fun convert(field: String, value: Any?): Any? {
        if (field !in fields || value == null) return value
        val amount = when {
            value is BigDecimal -> value
            value is Double && value.isFinite() -> BigDecimal.valueOf(value)
            else -> throw DataException("$PURPOSE requires finite Decimal or FLOAT64 field '$field'")
        }
        return try {
            amount.movePointRight(fractionDigits).longValueExact()
        } catch (exception: ArithmeticException) {
            throw DataException("$PURPOSE field '$field' has fractional minor units or exceeds INT64", exception)
        }
    }

    private fun minorSchema(field: String, schema: Schema): Schema {
        if (schema.type() != Schema.Type.FLOAT64 &&
            (schema.name() != Decimal.LOGICAL_NAME || schema.type() != Schema.Type.BYTES)
        ) {
            throw DataException("$PURPOSE requires Decimal or FLOAT64 schema for field '$field'")
        }
        val output = SchemaBuilder.int64().doc(schema.doc())
        if (schema.isOptional) output.optional()
        schema.defaultValue()?.let { output.defaultValue(convert(field, it)) }
        return output.build()
    }

    private fun copySchema(schema: Schema): Schema {
        schemaCache.get(schema)?.let { return it }
        val output = SchemaUtil.copySchemaBasics(schema)
        if (schema.isOptional) output.optional()
        for (field in schema.fields()) {
            output.field(field.name(), if (field.name() in fields) {
                minorSchema(field.name(), field.schema())
            } else {
                field.schema()
            })
        }
        val result = output.build()
        schemaCache.put(schema, result)
        return result
    }

    class Key<R : ConnectRecord<R>?> : MoneyTransform<R>() {
        override fun operatingSchema(record: R?): Schema? = record?.keySchema()
        override fun operatingValue(record: R?): Any? = record?.key()
        override fun newRecord(record: R?, schema: Schema?, value: Any?): R = record!!.newRecord(
            record.topic(), record.kafkaPartition(), schema, value,
            record.valueSchema(), record.value(), record.timestamp()
        )
    }

    class Value<R : ConnectRecord<R>?> : MoneyTransform<R>() {
        override fun operatingSchema(record: R?): Schema? = record?.valueSchema()
        override fun operatingValue(record: R?): Any? = record?.value()
        override fun newRecord(record: R?, schema: Schema?, value: Any?): R = record!!.newRecord(
            record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
            schema, value, record.timestamp()
        )
    }
}
