package org.phpinnacle.toblerone

import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.math.BigDecimal
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

internal class JsonTransformTest {
    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun serializesWholeSchemalessKeysAndValues(key: Boolean) {
        val input = mapOf("name" to "Привет\n\"test\"\\", "items" to listOf(1, true, null),
            "nested" to mapOf("id" to 42))
        for (config in listOf<Map<String?, String>>(emptyMap(), mapOf("fields" to ""))) {
            transformation(key, config).use { transform ->
                val original = record(null, input)
                val result = transform.apply(original)

                assertEquals(
                    """{"name":"Привет\n\"test\"\\","items":[1,true,null],"nested":{"id":42}}""",
                    targetValue(result, key)
                )
                assertNull(targetSchema(result, key))
                assertSame(input, targetValue(result, !key))
                assertRecordMetadata(original, result)
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun serializesWholeStructsWithoutSchemaEnvelope(key: Boolean) {
        val childSchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build()
        val schema = SchemaBuilder.struct().name("record").optional()
            .field("child", childSchema)
            .field("items", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build())
            .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
            .field("bytes", Schema.BYTES_SCHEMA).build()
        val input = Struct(schema).put("child", Struct(childSchema).put("id", 42))
            .put("items", listOf("value", null)).put("map", mapOf("count" to 2))
            .put("bytes", byteArrayOf(1, 2))

        transformation(key).use { transform ->
            val original = record(schema, input)
            val result = transform.apply(original)

            assertEquals(
                """{"child":{"id":42},"items":["value",null],"map":{"count":2},"bytes":"AQI="}""",
                targetValue(result, key)
            )
            assertEquals(Schema.OPTIONAL_STRING_SCHEMA, targetSchema(result, key))
            assertSame(schema, targetSchema(result, !key))
            assertSame(input, targetValue(result, !key))
            assertRecordMetadata(original, result)
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun serializesScalarsAndLogicalTypes(key: Boolean) {
        val cases = listOf(
            Triple(Schema.INT32_SCHEMA, 42, "42"),
            Triple(Schema.BOOLEAN_SCHEMA, true, "true"),
            Triple(Schema.STRING_SCHEMA, "text", "\"text\""),
            Triple(org.apache.kafka.connect.data.Date.SCHEMA, java.util.Date(0), "0"),
            Triple(Decimal.schema(2), BigDecimal("12.34"), "\"BNI=\"")
        )
        transformation(key).use { transform ->
            for ((schema, input, expected) in cases) {
                val result = transform.apply(record(schema, input))
                assertEquals(expected, targetValue(result, key))
                assertEquals(Schema.STRING_SCHEMA, targetSchema(result, key))
            }
            assertEquals("42", targetValue(transform.apply(record(null, 42)), key))
            assertEquals("\"text\"", targetValue(transform.apply(record(null, "text")), key))
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun serializesSelectedSchemalessFieldsWithoutChangingInput(key: Boolean) {
        val input = mapOf("object" to mapOf("id" to 42), "array" to listOf(1, 2),
            "text" to "hello", "number" to 42, "flag" to true, "nullable" to null, "untouched" to 7)
        transformation(key, mapOf("fields" to "object,array,text,number,flag,nullable,missing")).use { transform ->
            val original = record(null, input)
            val result = transform.apply(original)
            val output = targetValue(result, key) as Map<*, *>

            assertEquals(input + mapOf("object" to "{\"id\":42}", "array" to "[1,2]",
                "text" to "\"hello\"", "number" to "42", "flag" to "true"), output)
            assertFalse(output.containsKey("missing"))
            assertNull(targetSchema(result, key))
            assertEquals(42, input["number"])
            assertSame(input, targetValue(result, !key))
            assertRecordMetadata(original, result)
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun serializesSelectedSchemaFieldsAndDefaults(key: Boolean) {
        val childSchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build()
        val schema = SchemaBuilder.struct().name("record").version(1).doc("Record")
            .parameter("source", "test").optional()
            .field("child", childSchema)
            .field("items", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
            .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
            .field("nullable", Schema.OPTIONAL_INT32_SCHEMA)
            .field("default", SchemaBuilder.int32().optional().defaultValue(42).build())
            .field("date", org.apache.kafka.connect.data.Date.SCHEMA)
            .field("untouched", Schema.BOOLEAN_SCHEMA).build()
        val input = Struct(schema).put("child", Struct(childSchema).put("id", 42))
            .put("items", listOf(1, 2)).put("map", mapOf("id" to 3))
            .put("nullable", null).put("date", java.util.Date(0)).put("untouched", true)
        transformation(key, mapOf("fields" to "child,items,map,nullable,default,date,missing")).use { transform ->
            val original = record(schema, input)
            val result = transform.apply(original)
            val output = targetValue(result, key) as Struct
            val outputSchema = output.schema()

            assertEquals("{\"id\":42}", output.getString("child"))
            assertEquals("[1,2]", output.getString("items"))
            assertEquals("{\"id\":3}", output.getString("map"))
            assertNull(output.get("nullable"))
            assertEquals("42", output.getString("default"))
            assertEquals("0", output.getString("date"))
            assertEquals(Schema.STRING_SCHEMA, outputSchema.field("child").schema())
            assertEquals(Schema.STRING_SCHEMA, outputSchema.field("date").schema())
            assertEquals(Schema.OPTIONAL_STRING_SCHEMA, outputSchema.field("nullable").schema())
            assertEquals("42", outputSchema.field("default").schema().defaultValue())
            assertTrue(outputSchema.field("default").schema().isOptional)
            assertTrue(outputSchema.isOptional)
            assertEquals(schema.name(), outputSchema.name())
            assertEquals(schema.version(), outputSchema.version())
            assertEquals(schema.doc(), outputSchema.doc())
            assertEquals(schema.parameters(), outputSchema.parameters())
            assertEquals(schema.fields().map { it.name() }, outputSchema.fields().map { it.name() })
            assertSame(schema.field("untouched").schema(), outputSchema.field("untouched").schema())
            assertEquals(true, output.getBoolean("untouched"))
            output.validate()
            assertSame(outputSchema, targetSchema(transform.apply(original), key))
            assertSame(input, targetValue(result, !key))
            assertRecordMetadata(original, result)
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun serializesWholeSchemaDefaults(key: Boolean) {
        val schema = SchemaBuilder.int32().optional().defaultValue(42).build()
        transformation(key).use { transform ->
            val result = transform.apply(record(schema, 7))
            assertEquals("7", targetValue(result, key))
            assertEquals("42", targetSchema(result, key)!!.defaultValue())
            assertTrue(targetSchema(result, key)!!.isOptional)
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun passesThroughNullsInBothModes(key: Boolean) {
        for (config in listOf<Map<String?, String>>(emptyMap(), mapOf("fields" to "data"))) {
            transformation(key, config).use { transform ->
                for (schema in listOf(null, Schema.OPTIONAL_STRING_SCHEMA)) {
                    val original = record(schema, null)
                    assertSame(original, transform.apply(original))
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun requiresStructuredInputWhenFieldsAreSelected(key: Boolean) {
        transformation(key, mapOf("fields" to "data")).use { transform ->
            for (schema in listOf(null, Schema.INT32_SCHEMA)) {
                assertFailsWith<DataException> { transform.apply(record(schema, 42)) }
            }
        }
    }

    @Test
    fun rejectsUnsupportedSchemalessValues() {
        transformation(false).use { transform ->
            assertFailsWith<DataException> { transform.apply(record(null, Any())) }
        }
    }

    @Test
    fun keepsSchemaCachesSeparateAndClearsCacheOnReconfiguration() {
        val schema = SchemaBuilder.struct().field("a", Schema.INT32_SCHEMA).field("b", Schema.INT32_SCHEMA).build()
        val input = record(schema, Struct(schema).put("a", 1).put("b", 2))
        transformation(false, mapOf("fields" to "a")).use { first ->
            transformation(false, mapOf("fields" to "b")).use { second ->
                val firstOutput = first.apply(input).value() as Struct
                val secondOutput = second.apply(input).value() as Struct
                assertEquals("1", firstOutput.get("a"))
                assertEquals(2, firstOutput.get("b"))
                assertEquals(1, secondOutput.get("a"))
                assertEquals("2", secondOutput.get("b"))
                first.configure(mapOf("fields" to "b"))
                assertEquals(secondOutput, first.apply(input).value())
            }
        }
    }

    private fun transformation(key: Boolean, config: Map<String?, String> = emptyMap()): JsonTransform<SourceRecord> {
        val transform = if (key) JsonTransform.Key<SourceRecord>() else JsonTransform.Value<SourceRecord>()
        transform.configure(config)
        return transform
    }

    private fun record(schema: Schema?, value: Any?): SourceRecord =
        SourceRecord(mapOf("source" to "test"), mapOf("offset" to 1), "topic", 2,
            schema, value, schema, value, 123L).also { it.headers().addString("header", "value") }

    private fun targetValue(record: SourceRecord, key: Boolean): Any? = if (key) record.key() else record.value()

    private fun targetSchema(record: SourceRecord, key: Boolean): Schema? =
        if (key) record.keySchema() else record.valueSchema()

    private fun assertRecordMetadata(original: SourceRecord, result: SourceRecord) {
        assertEquals(original.topic(), result.topic())
        assertEquals(original.kafkaPartition(), result.kafkaPartition())
        assertEquals(original.timestamp(), result.timestamp())
        assertEquals(original.sourcePartition(), result.sourcePartition())
        assertEquals(original.sourceOffset(), result.sourceOffset())
        assertEquals(original.headers(), result.headers())
    }
}
