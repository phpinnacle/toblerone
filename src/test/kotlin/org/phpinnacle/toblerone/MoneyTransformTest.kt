package org.phpinnacle.toblerone

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import java.math.BigDecimal
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

internal class MoneyTransformTest {
    private val config = mapOf<String?, String>("fields" to "amount,balance,missing", "currency" to "RUB")

    @ParameterizedTest
    @CsvSource(
        "RUB, 1.2300, 123", "RUB, -1.2300, -123", "RUB, 0.0000, 0",
        "JPY, 123.0000, 123", "KWD, 1.2340, 1234", "CHF, 1.2300, 123",
        "RUB, 92233720368547758.0700, 9223372036854775807",
        "RUB, -92233720368547758.0800, -9223372036854775808"
    )
    fun convertsExactlyUsingCurrencyDigits(currency: String, amount: String, expected: Long) {
        for (key in listOf(true, false)) {
            transformation(key, config + ("currency" to currency)).use { transform ->
                val value = BigDecimal(amount)
                val schema = SchemaBuilder.struct().field("amount", Decimal.schema(4)).build()
                val schemaless = record(null, mapOf("amount" to value))
                val withSchema = record(schema, Struct(schema).put("amount", value))
                val mapOutput = targetValue(transform.apply(schemaless), key) as Map<*, *>
                val structOutput = targetValue(transform.apply(withSchema), key) as Struct

                assertEquals(expected, mapOutput["amount"])
                assertEquals(expected, structOutput.getInt64("amount"))
                structOutput.validate()
            }
        }
    }

    @ParameterizedTest
    @CsvSource("BYN, 0.29, 29", "BYN, 19.99, 1999", "BYN, -1.23, -123", "BYN, 0.0, 0",
        "JPY, 123.0, 123", "KWD, 1.234, 1234")
    fun convertsDoubleAmountsUsingCurrencyDigits(currency: String, amount: Double, expected: Long) {
        val schema = SchemaBuilder.struct().field("amount", Schema.FLOAT64_SCHEMA).build()
        for (key in listOf(true, false)) {
            transformation(key, config + ("currency" to currency)).use { transform ->
                val mapOutput = targetValue(transform.apply(record(null, mapOf("amount" to amount))), key) as Map<*, *>
                val structOutput = targetValue(transform.apply(record(schema, Struct(schema).put("amount", amount))),
                    key) as Struct

                assertEquals(expected, mapOutput["amount"])
                assertEquals(expected, structOutput.getInt64("amount"))
                structOutput.validate()
            }
        }
    }

    @Test
    fun preservesDoubleDefaultsAndOptionalNulls() {
        val schema = SchemaBuilder.struct()
            .field("amount", SchemaBuilder.float64().defaultValue(0.29).build())
            .field("balance", SchemaBuilder.float64().optional().defaultValue(1.23).build()).build()
        transformation(false, config + ("currency" to "BYN")).use { transform ->
            val output = transform.apply(record(schema, Struct(schema).put("balance", null))).value() as Struct

            assertEquals(29L, output.getInt64("amount"))
            assertEquals(29L, output.schema().field("amount").schema().defaultValue())
            assertEquals(123L, output.schema().field("balance").schema().defaultValue())
            assertNull(output.getWithoutDefault("balance"))
            assertTrue(output.schema().field("balance").schema().isOptional)
            output.validate()
        }
    }

    @Test
    fun rejectsDoubleFractionalMinorUnitsNonFiniteValuesAndOverflow() {
        val schema = SchemaBuilder.struct().field("amount", Schema.FLOAT64_SCHEMA).build()
        transformation(false, config + ("currency" to "BYN")).use { transform ->
            for (amount in listOf(1.234, -1.234, 0.1 + 0.2, Double.MAX_VALUE,
                Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)) {
                for (input in listOf(record(null, mapOf("amount" to amount)),
                    record(schema, Struct(schema).put("amount", amount)))) {
                    assertFailsWith<DataException>(amount.toString()) { transform.apply(input) }
                }
            }
            val defaultSchema = SchemaBuilder.struct()
                .field("amount", SchemaBuilder.float64().defaultValue(1.234).build()).build()
            assertFailsWith<DataException> {
                transform.apply(record(defaultSchema, Struct(defaultSchema).put("amount", 1.23)))
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun preservesSchemaMetadataNullsUnselectedFieldsAndRecordMetadata(key: Boolean) {
        val schema = SchemaBuilder.struct().name("row").version(2).doc("Row")
            .parameter("source", "test").optional()
            .field("amount", Decimal.builder(4).doc("Amount").build())
            .field("balance", Decimal.builder(4).optional().build())
            .field("untouched", Decimal.schema(4))
            .field("name", Schema.STRING_SCHEMA).build()
        val input = Struct(schema).put("amount", BigDecimal("1.2300")).put("balance", null)
            .put("untouched", BigDecimal("9.8765")).put("name", "test")
        val original = record(schema, input)
        transformation(key).use { transform ->
            val result = transform.apply(original)
            val output = targetValue(result, key) as Struct
            val outputSchema = output.schema()
            val amountSchema = outputSchema.field("amount").schema()

            assertEquals(123L, output.getInt64("amount"))
            assertNull(output.get("balance"))
            assertEquals(Schema.Type.INT64, amountSchema.type())
            assertNull(amountSchema.name())
            assertNull(amountSchema.parameters())
            assertEquals("Amount", amountSchema.doc())
            assertFalse(amountSchema.isOptional)
            assertTrue(outputSchema.field("balance").schema().isOptional)
            assertEquals(schema.name(), outputSchema.name())
            assertEquals(schema.version(), outputSchema.version())
            assertEquals(schema.doc(), outputSchema.doc())
            assertEquals(schema.parameters(), outputSchema.parameters())
            assertTrue(outputSchema.isOptional)
            assertEquals(schema.fields().map { it.name() }, outputSchema.fields().map { it.name() })
            for (name in listOf("untouched", "name")) {
                assertSame(schema.field(name).schema(), outputSchema.field(name).schema())
                assertEquals(input.get(name), output.get(name))
            }
            assertSame(input, targetValue(result, !key))
            assertSame(schema, targetSchema(result, !key))
            assertEquals(BigDecimal("1.2300"), input.get("amount"))
            assertSame(outputSchema, targetSchema(transform.apply(original), key))
            assertEquals(original.topic(), result.topic())
            assertEquals(original.kafkaPartition(), result.kafkaPartition())
            assertEquals(original.timestamp(), result.timestamp())
            assertEquals(original.sourcePartition(), result.sourcePartition())
            assertEquals(original.sourceOffset(), result.sourceOffset())
            assertEquals(original.headers(), result.headers())
            output.validate()
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun preservesSchemalessNullsAndMissingFields(key: Boolean) {
        val input = mapOf("amount" to BigDecimal("1.2300"), "balance" to null, "untouched" to "1.23")
        transformation(key).use { transform ->
            val output = transform.apply(record(null, input))
            assertEquals(input + ("amount" to 123L), targetValue(output, key))
            assertNull(targetSchema(output, key))
            assertEquals(BigDecimal("1.2300"), input["amount"])
        }
    }

    @Test
    fun convertsDefaultsAndClearsCachedSchemasWhenReconfigured() {
        val schema = SchemaBuilder.struct()
            .field("amount", Decimal.builder(4).defaultValue(BigDecimal("1.2300")).build())
            .field("balance", Decimal.builder(4).optional().defaultValue(BigDecimal("2.3400")).build()).build()
        val input = record(schema, Struct(schema).put("balance", null))
        transformation(false).use { first ->
            transformation(false, config + ("currency" to "KWD")).use { second ->
                val rub = first.apply(input).value() as Struct
                assertEquals(123L, rub.getInt64("amount"))
                assertEquals(234L, rub.schema().field("balance").schema().defaultValue())
                assertNull(rub.getWithoutDefault("balance"))
                assertEquals(1230L, (second.apply(input).value() as Struct).getInt64("amount"))
                first.configure(config + ("currency" to "KWD"))
                val kwd = first.apply(input).value() as Struct
                assertEquals(1230L, kwd.getInt64("amount"))
                assertEquals(2340L, kwd.schema().field("balance").schema().defaultValue())
                assertEquals(123L, rub.getInt64("amount"))
                rub.validate()
                kwd.validate()
            }
        }
    }

    @ParameterizedTest
    @CsvSource(
        "RUB, 1.2340", "RUB, -1.2340", "JPY, 1.2300", "KWD, 1.2345",
        "RUB, 92233720368547758.0800", "RUB, -92233720368547758.0900"
    )
    fun rejectsFractionalMinorUnitsAndOverflow(currency: String, amount: String) {
        val value = BigDecimal(amount)
        val schema = SchemaBuilder.struct().field("amount", Decimal.schema(4)).build()
        transformation(false, config + ("currency" to currency)).use { transform ->
            for (input in listOf(record(null, mapOf("amount" to value)),
                record(schema, Struct(schema).put("amount", value)))) {
                val exception = assertFailsWith<DataException> { transform.apply(input) }
                assertTrue(exception.cause is ArithmeticException)
            }
        }
    }

    @Test
    fun rejectsWrongTypesAndUnsupportedSchemas() {
        transformation(false).use { transform ->
            for (value in listOf("1.23", 1.23f, 123L, byteArrayOf(123))) {
                assertFailsWith<DataException> { transform.apply(record(null, mapOf("amount" to value))) }
            }
            for (fieldSchema in listOf(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_BYTES_SCHEMA,
                Schema.OPTIONAL_INT64_SCHEMA)) {
                val schema = SchemaBuilder.struct().field("amount", fieldSchema).build()
                assertFailsWith<DataException> { transform.apply(record(schema, Struct(schema))) }
            }
            for (schema in listOf(null, Decimal.schema(2))) {
                assertFailsWith<DataException> { transform.apply(record(schema, BigDecimal("1.23"))) }
            }
        }
    }

    @Test
    fun rejectsDefaultsThatCannotBeConvertedExactly() {
        val schema = SchemaBuilder.struct()
            .field("amount", Decimal.builder(4).defaultValue(BigDecimal("1.2340")).build()).build()
        transformation(false).use { transform ->
            assertFailsWith<DataException> {
                transform.apply(record(schema, Struct(schema).put("amount", BigDecimal("1.2300"))))
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun preservesNullKeysAndValues(key: Boolean) {
        transformation(key).use { transform ->
            for (schema in listOf(null, SchemaBuilder.struct().optional().build())) {
                val input = record(schema, null)
                assertSame(input, transform.apply(input))
            }
        }
    }

    @Test
    fun validatesConfigurationAndAcceptsExplicitMinorMode() {
        val invalid = listOf(
            config - "fields", config - "currency",
            config + ("fields" to null), config + ("fields" to ""), config + ("fields" to "amount, "),
            config + ("fields" to listOf("amount", 1)),
            config + ("currency" to null), config + ("currency" to ""), config + ("currency" to "rub"),
            config + ("currency" to "RUBL"), config + ("currency" to "ZZZ"),
            config + ("mode" to "major"), config + ("mode" to ""), config + ("mode" to null)
        )
        for (props in invalid) {
            MoneyTransform.Value<SourceRecord>().use { transform ->
                assertFailsWith<ConfigException>(props.toString()) { transform.configure(props) }
            }
        }
        transformation(false, config + mapOf("mode" to "minor", "fields" to " amount , balance ")).use {
            assertEquals(mapOf("amount" to 123L), it.apply(record(null,
                mapOf("amount" to BigDecimal("1.2300")))).value())
        }
    }

    private fun transformation(key: Boolean, props: Map<String?, *> = config): MoneyTransform<SourceRecord> {
        val transform = if (key) MoneyTransform.Key<SourceRecord>() else MoneyTransform.Value<SourceRecord>()
        transform.configure(props)
        return transform
    }

    private fun record(schema: Schema?, value: Any?): SourceRecord =
        SourceRecord(mapOf("source" to "test"), mapOf("offset" to 1), "topic", 2,
            schema, value, schema, value, 123L).also { it.headers().addString("header", "value") }

    private fun targetValue(record: SourceRecord, key: Boolean): Any? = if (key) record.key() else record.value()

    private fun targetSchema(record: SourceRecord, key: Boolean): Schema? =
        if (key) record.keySchema() else record.valueSchema()
}
