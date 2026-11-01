package org.phpinnacle.toblerone

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

internal class EmptyStringToNullTransformTest {
    private val config = mapOf<String?, String>("fields" to " empty, text, whitespace, nullable, count, missing ")

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun convertsOnlySelectedEmptyStringsInSchemalessKeysAndValues(key: Boolean) {
        val input = mapOf("empty" to "", "text" to "hello", "whitespace" to " \t\n",
            "nullable" to null, "count" to 0, "untouched" to "", "nested" to mapOf("empty" to ""))
        transformation(key).use { transform ->
            val original = record(null, input)
            val result = transform.apply(original)
            val output = targetValue(result, key) as Map<*, *>

            assertEquals(input + ("empty" to null), output)
            assertFalse(output.containsKey("missing"))
            assertEquals("", input["empty"])
            assertNull(targetSchema(result, key))
            assertSame(input, targetValue(result, !key))
            assertMetadata(original, result)
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun optionallyTrimsSelectedStringsInMapsAndStructs(key: Boolean) {
        val schema = SchemaBuilder.struct().field("empty", Schema.STRING_SCHEMA)
            .field("text", Schema.STRING_SCHEMA).field("whitespace", Schema.STRING_SCHEMA)
            .field("nullable", Schema.OPTIONAL_STRING_SCHEMA).field("count", Schema.INT32_SCHEMA)
            .field("untouched", Schema.STRING_SCHEMA).build()
        val input = mapOf("empty" to "", "text" to "  hello world \t", "whitespace" to " \t\n",
            "nullable" to null, "count" to 42, "untouched" to "  keep  ")
        transformation(key).use { transform ->
            for (trim in listOf(true, false)) {
                transform.configure(config + ("trim" to trim.toString()))
                val expected = input + if (trim) {
                    mapOf("empty" to null, "text" to "hello world", "whitespace" to null)
                } else {
                    mapOf("empty" to null)
                }
                assertEquals(expected, targetValue(transform.apply(record(null, input)), key))
                val value = Struct(schema)
                input.forEach { (name, item) -> value.put(name, item) }
                val output = targetValue(transform.apply(record(schema, value)), key) as Struct
                assertEquals(expected, schema.fields().associate { it.name() to output.get(it.name()) })
                output.validate()
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun makesSelectedStringsNullableAndPreservesMetadata(key: Boolean) {
        val stringSchema = SchemaBuilder.string().name("text").version(2).doc("Text")
            .parameter("source", "varchar").build()
        val schema = SchemaBuilder.struct().name("row").version(1).doc("Row")
            .parameter("source", "test").optional()
            .field("empty", stringSchema).field("text", Schema.STRING_SCHEMA)
            .field("whitespace", Schema.STRING_SCHEMA).field("nullable", Schema.OPTIONAL_STRING_SCHEMA)
            .field("count", Schema.INT32_SCHEMA).field("untouched", Schema.STRING_SCHEMA).build()
        val input = Struct(schema).put("empty", "").put("text", "hello").put("whitespace", " ")
            .put("nullable", null).put("count", 42).put("untouched", "")
        transformation(key).use { transform ->
            val original = record(schema, input)
            val result = transform.apply(original)
            val output = targetValue(result, key) as Struct
            val outputSchema = targetSchema(result, key)!!
            val emptySchema = outputSchema.field("empty").schema()

            assertNull(output.get("empty"))
            assertNull(output.get("nullable"))
            for (name in listOf("text", "whitespace", "count", "untouched")) {
                assertEquals(input.get(name), output.get(name))
            }
            assertTrue(emptySchema.isOptional)
            assertTrue(outputSchema.field("text").schema().isOptional)
            assertEquals(stringSchema.type(), emptySchema.type())
            assertEquals(stringSchema.name(), emptySchema.name())
            assertEquals(stringSchema.version(), emptySchema.version())
            assertEquals(stringSchema.doc(), emptySchema.doc())
            assertEquals(stringSchema.parameters(), emptySchema.parameters())
            assertEquals(schema.name(), outputSchema.name())
            assertEquals(schema.version(), outputSchema.version())
            assertEquals(schema.doc(), outputSchema.doc())
            assertEquals(schema.parameters(), outputSchema.parameters())
            assertTrue(outputSchema.isOptional)
            assertEquals(schema.fields().map { it.name() }, outputSchema.fields().map { it.name() })
            assertSame(schema.field("count").schema(), outputSchema.field("count").schema())
            assertSame(schema.field("untouched").schema(), outputSchema.field("untouched").schema())
            assertEquals("", input.get("empty"))
            assertSame(input, targetValue(result, !key))
            assertSame(schema, targetSchema(result, !key))
            assertSame(outputSchema, targetSchema(transform.apply(original), key))
            output.validate()
            assertMetadata(original, result)
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun defaultsCannotReplaceConvertedOrExistingNulls(key: Boolean) {
        val schema = SchemaBuilder.struct()
            .field("empty", SchemaBuilder.string().defaultValue("fallback").build())
            .field("text", SchemaBuilder.string().defaultValue("hello").build())
            .field("whitespace", SchemaBuilder.string().defaultValue("").build())
            .field("nullable", SchemaBuilder.string().optional().defaultValue("fallback").build()).build()
        val input = Struct(schema).put("empty", "").put("nullable", null)
        transformation(key).use { transform ->
            val result = transform.apply(record(schema, input))
            val output = targetValue(result, key) as Struct

            assertNull(output.get("empty"))
            assertEquals("hello", output.get("text"))
            assertNull(output.get("whitespace"))
            assertNull(output.get("nullable"))
            output.schema().fields().forEach { assertNull(it.schema().defaultValue()) }
            assertFalse(output.schema().isOptional)
            output.validate()
            JsonConverter().use { converter ->
                converter.configure(mapOf("schemas.enable" to false), key)
                val json = converter.fromConnectData("topic", output.schema(), output).toString(Charsets.UTF_8)
                assertEquals("{\"empty\":null,\"text\":\"hello\",\"whitespace\":null,\"nullable\":null}", json)
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun preservesNullRecordsAndRejectsUnstructuredInput(key: Boolean) {
        transformation(key).use { transform ->
            for (schema in listOf(null, SchemaBuilder.struct().optional().build())) {
                val original = record(schema, null)
                assertSame(original, transform.apply(original))
            }
            for (schema in listOf(null, Schema.STRING_SCHEMA)) {
                assertFailsWith<DataException> { transform.apply(record(schema, "")) }
            }
        }
    }

    @Test
    fun requiresNonemptyFieldNames() {
        EmptyStringToNullTransform.Value<SourceRecord>().use { transform ->
            assertFailsWith<ConfigException> { transform.configure(emptyMap<String?, String>()) }
            for (fields in listOf(null, "", " ", "empty,,text", listOf("empty", " "))) {
                assertFailsWith<ConfigException> { transform.configure(mapOf("fields" to fields)) }
            }
        }
    }

    @Test
    fun rejectsInvalidTrimSetting() {
        EmptyStringToNullTransform.Value<SourceRecord>().use { transform ->
            assertFailsWith<ConfigException> { transform.configure(config + ("trim" to "yes")) }
        }
    }

    @Test
    fun clearsSchemaCacheWhenFieldsChange() {
        val schema = SchemaBuilder.struct().field("empty", Schema.STRING_SCHEMA)
            .field("text", Schema.STRING_SCHEMA).build()
        val original = record(schema, Struct(schema).put("empty", "").put("text", ""))
        transformation(false).use { transform ->
            transform.apply(original)
            transform.configure(mapOf("fields" to "text"))
            val output = transform.apply(original).value() as Struct
            assertEquals("", output.get("empty"))
            assertFalse(output.schema().field("empty").schema().isOptional)
            assertNull(output.get("text"))
            assertTrue(output.schema().field("text").schema().isOptional)
        }
    }

    private fun transformation(key: Boolean): EmptyStringToNullTransform<SourceRecord> {
        val transform = if (key) {
            EmptyStringToNullTransform.Key<SourceRecord>()
        } else {
            EmptyStringToNullTransform.Value<SourceRecord>()
        }
        transform.configure(config)
        return transform
    }

    private fun record(schema: Schema?, value: Any?): SourceRecord =
        SourceRecord(mapOf("source" to "test"), mapOf("offset" to 1), "topic", 2,
            schema, value, schema, value, 123L).also { it.headers().addString("header", "value") }

    private fun targetValue(record: SourceRecord, key: Boolean): Any? = if (key) record.key() else record.value()

    private fun targetSchema(record: SourceRecord, key: Boolean): Schema? =
        if (key) record.keySchema() else record.valueSchema()

    private fun assertMetadata(original: SourceRecord, result: SourceRecord) {
        assertEquals(original.topic(), result.topic())
        assertEquals(original.kafkaPartition(), result.kafkaPartition())
        assertEquals(original.timestamp(), result.timestamp())
        assertEquals(original.sourcePartition(), result.sourcePartition())
        assertEquals(original.sourceOffset(), result.sourceOffset())
        assertEquals(original.headers(), result.headers())
    }
}
