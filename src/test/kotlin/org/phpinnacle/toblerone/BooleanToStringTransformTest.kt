package org.phpinnacle.toblerone

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
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

internal class BooleanToStringTransformTest {
    private val config = mapOf<String?, String>(
        "fields" to "active,inactive,nullable,name,count,missing",
        "true" to "yes",
        "false" to "no"
    )

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun convertsSelectedFieldsInSchemalessKeysAndValues(key: Boolean) {
        val input = mapOf(
            "active" to true,
            "inactive" to false,
            "nullable" to null,
            "name" to "true",
            "count" to 1,
            "untouched" to true
        )
        transformation(key).use { transform ->
            val original = record(null, input)
            val result = transform.apply(original)
            val output = targetValue(result, key) as Map<*, *>

            assertEquals(input + mapOf("active" to "yes", "inactive" to "no"), output)
            assertFalse(output.containsKey("missing"))
            assertNull(targetSchema(result, key))
            assertEquals(true, input["active"])
            assertEquals(false, input["inactive"])
            assertSame(input, targetValue(result, !key))
            assertRecordMetadata(original, result)
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun convertsSchemaAndPreservesMetadataForKeysAndValues(key: Boolean) {
        val booleanSchema = SchemaBuilder.bool().name("flag").version(2).doc("Flag")
            .parameter("source", "boolean").build()
        val schema = SchemaBuilder.struct().name("record").version(1).doc("Record")
            .parameter("source", "test").optional()
            .field("active", booleanSchema)
            .field("inactive", Schema.BOOLEAN_SCHEMA)
            .field("nullable", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("count", Schema.INT32_SCHEMA)
            .field("untouched", Schema.BOOLEAN_SCHEMA).build()
        val input = Struct(schema).put("active", true).put("inactive", false)
            .put("nullable", null).put("name", "true").put("count", 1).put("untouched", true)

        transformation(key).use { transform ->
            val original = record(schema, input)
            val result = transform.apply(original)
            val outputSchema = targetSchema(result, key)!!
            val output = targetValue(result, key) as Struct
            val activeSchema = outputSchema.field("active").schema()

            assertEquals(schema.name(), outputSchema.name())
            assertEquals(schema.version(), outputSchema.version())
            assertEquals(schema.doc(), outputSchema.doc())
            assertEquals(schema.parameters(), outputSchema.parameters())
            assertTrue(outputSchema.isOptional)
            assertEquals(schema.fields().map { it.name() }, outputSchema.fields().map { it.name() })
            assertEquals(Schema.Type.STRING, activeSchema.type())
            assertFalse(activeSchema.isOptional)
            assertEquals(booleanSchema.name(), activeSchema.name())
            assertEquals(booleanSchema.version(), activeSchema.version())
            assertEquals(booleanSchema.doc(), activeSchema.doc())
            assertEquals(booleanSchema.parameters(), activeSchema.parameters())
            assertEquals(Schema.STRING_SCHEMA, outputSchema.field("inactive").schema())
            assertEquals(Schema.OPTIONAL_STRING_SCHEMA, outputSchema.field("nullable").schema())
            assertEquals("yes", output.getString("active"))
            assertEquals("no", output.getString("inactive"))
            assertNull(output.get("nullable"))
            for (name in listOf("name", "count", "untouched")) {
                assertSame(schema.field(name).schema(), outputSchema.field(name).schema())
                assertEquals(input.get(name), output.get(name))
            }
            output.validate()
            assertEquals(true, input.getBoolean("active"))
            assertSame(input, targetValue(result, !key))
            assertSame(schema, targetSchema(result, !key))
            assertSame(outputSchema, targetSchema(transform.apply(original), key))
            assertRecordMetadata(original, result)
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun convertsBooleanDefaults(key: Boolean) {
        val schema = SchemaBuilder.struct()
            .field("active", SchemaBuilder.bool().defaultValue(true).build())
            .field("inactive", SchemaBuilder.bool().optional().defaultValue(false).build()).build()
        transformation(key).use { transform ->
            val output = targetValue(transform.apply(record(schema, Struct(schema))), key) as Struct

            assertEquals("yes", output.getString("active"))
            assertEquals("no", output.getString("inactive"))
            assertEquals("yes", output.schema().field("active").schema().defaultValue())
            assertEquals("no", output.schema().field("inactive").schema().defaultValue())
            assertTrue(output.schema().field("inactive").schema().isOptional)
            assertFalse(output.schema().isOptional)
            output.validate()
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun passesThroughNullKeysAndValues(key: Boolean) {
        transformation(key).use { transform ->
            for (schema in listOf(null, SchemaBuilder.struct().optional().build())) {
                val original = record(schema, null)
                assertSame(original, transform.apply(original))
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun requiresStructuredInput(key: Boolean) {
        transformation(key).use { transform ->
            for (schema in listOf(null, Schema.BOOLEAN_SCHEMA)) {
                assertFailsWith<DataException> { transform.apply(record(schema, true)) }
            }
        }
    }

    @Test
    fun requiresBothStringMappings() {
        BooleanToStringTransform.Value<SourceRecord>().use { transform ->
            for (name in listOf("true", "false")) {
                assertFailsWith<ConfigException> { transform.configure(config - name) }
                assertFailsWith<ConfigException> { transform.configure(config + (name to null)) }
            }
        }
    }

    @Test
    fun acceptsEmptyStrings() {
        BooleanToStringTransform.Value<SourceRecord>().use { transform ->
            transform.configure(config + mapOf("true" to "", "false" to ""))
            val result = transform.apply(record(null, mapOf("active" to true, "inactive" to false)))
            assertEquals(mapOf("active" to "", "inactive" to ""), result.value())
        }
    }

    @Test
    fun leavesFieldsUnchangedWhenNoneAreSelected() {
        BooleanToStringTransform.Value<SourceRecord>().use { transform ->
            transform.configure(config - "fields")
            val input = mapOf("active" to true)
            assertEquals(input, transform.apply(record(null, input)).value())
        }
    }

    @Test
    fun keepsSchemaCachesSeparateAndClearsCacheOnReconfiguration() {
        val schema = SchemaBuilder.struct()
            .field("active", SchemaBuilder.bool().defaultValue(true).build())
            .field("inactive", Schema.BOOLEAN_SCHEMA).build()
        val input = record(schema, Struct(schema).put("inactive", false))
        transformation(false).use { first ->
            BooleanToStringTransform.Value<SourceRecord>().use { second ->
                val firstSchema = first.apply(input).valueSchema()
                second.configure(config + mapOf("fields" to "inactive", "false" to "disabled"))
                val output = second.apply(input).value() as Struct

                assertEquals(true, output.getBoolean("active"))
                assertEquals("disabled", output.getString("inactive"))
                first.configure(config + ("true" to "enabled"))
                val reconfigured = first.apply(input).value() as Struct
                assertEquals("enabled", reconfigured.getString("active"))
                assertEquals("enabled", reconfigured.schema().field("active").schema().defaultValue())
                assertEquals("yes", firstSchema.field("active").schema().defaultValue())
            }
        }
    }

    private fun transformation(key: Boolean): BooleanToStringTransform<SourceRecord> {
        val transform = if (key) {
            BooleanToStringTransform.Key<SourceRecord>()
        } else {
            BooleanToStringTransform.Value<SourceRecord>()
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

    private fun assertRecordMetadata(original: SourceRecord, result: SourceRecord) {
        assertEquals(original.topic(), result.topic())
        assertEquals(original.kafkaPartition(), result.kafkaPartition())
        assertEquals(original.timestamp(), result.timestamp())
        assertEquals(original.sourcePartition(), result.sourcePartition())
        assertEquals(original.sourceOffset(), result.sourceOffset())
        assertEquals(original.headers(), result.headers())
    }
}
