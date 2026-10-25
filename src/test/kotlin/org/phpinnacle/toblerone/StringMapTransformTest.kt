package org.phpinnacle.toblerone

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.runtime.ConnectorConfig
import org.apache.kafka.connect.runtime.isolation.Plugins
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertSame

internal class StringMapTransformTest {
    private val config = mapOf<String?, String>(
        "field" to "status",
        "mapping.draft" to "черновик",
        "mapping.published" to "опубликовано",
        "mapping.deleted" to "",
        "mapping." to "empty",
        "mapping.a,b;c.d" to "punctuation",
        "mapping. draft " to " padded ",
        "mapping.черновик" to "no cascading",
        "mapping.1" to "one",
        "mapping.true" to "boolean string"
    )

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun mapsExactStringsInSchemalessKeysAndValues(key: Boolean) {
        val cases = listOf(
            "draft" to "черновик", "published" to "опубликовано", "deleted" to "",
            "" to "empty", "a,b;c.d" to "punctuation", " draft " to " padded ",
            "unknown" to "unknown", "Draft" to "Draft", "draft!" to "draft!",
            1 to 1, true to true, null to null
        )
        transformation(key).use { transform ->
            for ((input, expected) in cases) {
                val value = mapOf("status" to input, "other" to "draft")
                val original = record(null, value)
                val result = transform.apply(original)

                assertEquals(value + ("status" to expected), targetValue(result, key))
                assertNull(targetSchema(result, key))
                assertEquals(input, value["status"])
                assertSame(value, targetValue(result, !key))
                assertRecordMetadata(original, result)
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun mapsFieldsAndPreservesSchemasForKeysAndValues(key: Boolean) {
        val stringSchema = SchemaBuilder.string().name("status").doc("Status").version(2)
            .parameter("source", "test").optional().build()
        val cases = listOf(
            Triple(stringSchema, "draft", "черновик"),
            Triple(Schema.STRING_SCHEMA, "published", "опубликовано"),
            Triple(stringSchema, "deleted", ""),
            Triple(stringSchema, "unknown", "unknown"),
            Triple(stringSchema, null, null),
            Triple(SchemaBuilder.string().optional().defaultValue("draft").build(), null, "черновик"),
            Triple(Schema.INT32_SCHEMA, 1, 1),
            Triple(Schema.BOOLEAN_SCHEMA, true, true)
        )
        transformation(key).use { transform ->
            for ((fieldSchema, input, expected) in cases) {
                val schema = SchemaBuilder.struct().name("record").doc("Record").version(1).optional()
                    .parameter("source", "test")
                    .field("status", fieldSchema).field("other", Schema.STRING_SCHEMA).build()
                val value = Struct(schema).put("status", input).put("other", "draft")
                val original = record(schema, value)
                val result = transform.apply(original)
                val output = targetValue(result, key) as Struct

                assertSame(schema, targetSchema(result, key))
                assertEquals(expected, output.get("status"))
                assertEquals("draft", output.getString("other"))
                assertEquals(input ?: fieldSchema.defaultValue(), value.get("status"))
                assertSame(value, targetValue(result, !key))
                assertSame(schema, targetSchema(result, !key))
                output.validate()
                assertRecordMetadata(original, result)
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun leavesMissingFieldsUnchanged(key: Boolean) {
        transformation(key).use { transform ->
            val value = mapOf("other" to "draft")
            val output = targetValue(transform.apply(record(null, value)), key) as Map<*, *>
            assertEquals(value, output)
            assertFalse(output.containsKey("status"))

            val schema = SchemaBuilder.struct().field("other", Schema.STRING_SCHEMA).build()
            val input = Struct(schema).put("other", "draft")
            val result = transform.apply(record(schema, input))
            assertSame(schema, targetSchema(result, key))
            assertEquals(input, targetValue(result, key))
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
            for (schema in listOf(null, Schema.STRING_SCHEMA)) {
                assertFailsWith<DataException> { transform.apply(record(schema, "draft")) }
            }
        }
    }

    @Test
    fun requiresNonemptyFieldAndAtLeastOneMapping() {
        StringMapTransform.Value<SourceRecord>().use { transform ->
            assertFailsWith<ConfigException> { transform.configure(config - "field") }
            for (field in listOf(null, "", " ")) {
                assertFailsWith<ConfigException> { transform.configure(config + ("field" to field)) }
            }
            assertFailsWith<ConfigException> { transform.configure(mapOf("field" to "status")) }
        }
    }

    @Test
    fun rejectsNonStringMappings() {
        StringMapTransform.Value<SourceRecord>().use { transform ->
            for (target in listOf(null, 1, true, listOf("draft"))) {
                assertFailsWith<ConfigException> { transform.configure(config + ("mapping.draft" to target)) }
            }
        }
    }

    @Test
    fun replacesConfigurationOnReconfigure() {
        transformation(false).use { transform ->
            transform.configure(mapOf("field" to "other", "mapping.draft" to "new"))
            val result = transform.apply(record(null, mapOf("status" to "draft", "other" to "draft")))
            assertEquals(mapOf("status" to "draft", "other" to "new"), result.value())
            assertEquals(mapOf("other" to "published"),
                transform.apply(record(null, mapOf("other" to "published"))).value())
        }
    }

    @Test
    fun acceptsDynamicMappingsThroughKafkaConnectorConfig() {
        val plugins = Plugins(mapOf("plugin.discovery" to "service_load"))
        plugins.delegatingLoader().use {
            val props = mapOf(
                "name" to "test",
                "connector.class" to "unused",
                "transforms" to "map",
                "transforms.map.type" to StringMapTransform.Value::class.java.name,
                "transforms.map.field" to "status",
                "transforms.map.mapping.draft" to "черновик",
                "transforms.map.mapping." to "empty"
            )
            val connectorConfig = ConnectorConfig(plugins, props)
            connectorConfig.transformationStages<SourceRecord>().single().use { stage ->
                assertEquals(mapOf("status" to "черновик"),
                    stage.apply(record(null, mapOf("status" to "draft"))).value())
                assertEquals(mapOf("status" to "empty"),
                    stage.apply(record(null, mapOf("status" to ""))).value())
            }
        }
    }

    private fun transformation(key: Boolean): StringMapTransform<SourceRecord> {
        val transform = if (key) {
            StringMapTransform.Key<SourceRecord>()
        } else {
            StringMapTransform.Value<SourceRecord>()
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
