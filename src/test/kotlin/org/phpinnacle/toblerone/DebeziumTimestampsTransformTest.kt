package org.phpinnacle.toblerone

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.transforms.ExtractField
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Instant
import java.util.Date
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

internal class DebeziumTimestampsTransformTest {
    private val millis = 1_730_000_000_123L
    private val transform = DebeziumTimestampsTransform<SourceRecord>().apply { configure(emptyMap<String?, Any>()) }

    @ParameterizedTest
    @ValueSource(strings = ["c", "r", "u"])
    fun addsMissingFieldsBeforeExtractingAfter(op: String) {
        val original = event(op)
        val result = transform.apply(original)
        val output = result.value() as Struct
        val after = output.getStruct("after")

        assertEquals(if (op == "u") null else Date(millis), after.get("created_at"))
        assertEquals(Date(millis), after.get("updated_at"))
        assertEquals(42, after.getInt32("id"))
        after.validate()
        output.validate()
        assertEquals("products.Value", after.schema().name())
        assertEquals("Row", after.schema().doc())
        assertEquals(2, after.schema().version())
        assertEquals(mapOf("source" to "products"), after.schema().parameters())
        assertTrue(after.schema().isOptional)
        assertSame(after.schema(), output.schema().field("before").schema())
        assertNull((original.value() as Struct).getStruct("after").schema().field("created_at"))
        assertEquals(original.sourcePartition(), result.sourcePartition())
        assertEquals(original.sourceOffset(), result.sourceOffset())
        assertSame(original.key(), result.key())
        assertSame(original.keySchema(), result.keySchema())
        assertEquals(original.topic(), result.topic())
        assertEquals(original.kafkaPartition(), result.kafkaPartition())
        assertEquals(original.timestamp(), result.timestamp())
        assertEquals(original.headers(), result.headers())

        ExtractField.Value<SourceRecord>().use { unwrap ->
            unwrap.configure(mapOf("field" to "after"))
            assertSame(after, unwrap.apply(result).value())
        }
        JsonConverter().use { converter ->
            converter.configure(mapOf("schemas.enable" to true), false)
            val bytes = converter.fromConnectData(result.topic(), result.valueSchema(), result.value())
            val decoded = converter.toConnectData(result.topic(), bytes).value() as Struct
            assertEquals(after, decoded.getStruct("after"))
        }
    }

    @Test
    fun keepsBeforeValuesAndUsesTheSameRowSchema() {
        val original = event("u", before = true)
        val output = transform.apply(original).value() as Struct
        val before = output.getStruct("before")
        assertEquals(41, before.getInt32("id"))
        assertNull(before.get("created_at"))
        assertNull(before.get("updated_at"))
        assertSame(output.getStruct("after").schema(), before.schema())
        before.validate()
        output.validate()
    }

    @Test
    fun fillsNullFieldsInTheirExistingTimestampTypes() {
        val types = listOf(
            Timestamp.builder().optional().build() to Date(millis),
            SchemaBuilder.int64().name("io.debezium.time.Timestamp").optional().build() to millis,
            SchemaBuilder.int64().name("io.debezium.time.MicroTimestamp").optional().build() to millis * 1_000,
            SchemaBuilder.string().name("io.debezium.time.ZonedTimestamp").optional().build() to
                Instant.ofEpochMilli(millis).toString()
        )
        for ((schema, expected) in types) {
            val rowSchema = rowSchema(schema)
            val original = event("c", rowSchema)
            val output = (transform.apply(original).value() as Struct).getStruct("after")
            assertEquals(expected, output.get("created_at"))
            assertEquals(expected, output.get("updated_at"))
            assertSame(schema, output.schema().field("updated_at").schema())
            output.validate()
        }
    }

    @Test
    fun preservesExistingValuesOnCreateReadAndUpdate() {
        val schema = rowSchema(Schema.OPTIONAL_STRING_SCHEMA)
        for (op in listOf("c", "r", "u")) {
            val original = event(op, schema)
            (original.value() as Struct).getStruct("after")
                .put("created_at", "source-created").put("updated_at", "source-updated")
            val output = (transform.apply(original).value() as Struct).getStruct("after")
            assertEquals("source-created", output.getString("created_at"))
            assertEquals("source-updated", output.getString("updated_at"))
            output.validate()
        }
    }

    @Test
    fun doesNotInventCreatedAtOnUpdate() {
        val original = event("u", rowSchema(Timestamp.builder().optional().build()))
        val output = (transform.apply(original).value() as Struct).getStruct("after")
        assertNull(output.get("created_at"))
        assertEquals(Date(millis), output.get("updated_at"))
    }

    @Test
    fun preservesSourceCreationTimeWhenOnlyUpdatedAtIsMissing() {
        val sourceTimestamp = SchemaBuilder.int64().name("io.debezium.time.MicroTimestamp").optional().build()
        val row = SchemaBuilder.struct().optional().field("id", Schema.INT32_SCHEMA)
            .field("created_at", sourceTimestamp).build()
        val original = event("u", row)
        (original.value() as Struct).getStruct("after").put("created_at", 123_456L)
        val output = (transform.apply(original).value() as Struct).getStruct("after")
        assertEquals(123_456L, output.getInt64("created_at"))
        assertSame(sourceTimestamp, output.schema().field("created_at").schema())
        assertEquals(Date(millis), output.get("updated_at"))
        output.validate()
    }

    @Test
    fun usesProcessingClockWhenEnvelopeTimestampIsNull() {
        val original = event("c")
        (original.value() as Struct).put("ts_ms", null)
        val start = System.currentTimeMillis()
        val output = (transform.apply(original).value() as Struct).getStruct("after")
        val end = System.currentTimeMillis()
        val created = output.get("created_at") as Date
        assertTrue(created.time in start..end)
        assertEquals(created, output.get("updated_at"))
    }

    @Test
    fun preservesTombstonesDeletesAndNonRowEvents() {
        val tombstone = SourceRecord(null, null, "products", null, null)
        assertSame(tombstone, transform.apply(tombstone))
        for (op in listOf("d", "t", "m", "unknown")) {
            val original = event(op)
            assertSame(original, transform.apply(original))
        }
        val withoutAfter = event("u")
        (withoutAfter.value() as Struct).put("after", null)
        assertSame(withoutAfter, transform.apply(withoutAfter))
        val heartbeat = SourceRecord(null, null, "heartbeat", null, mapOf("ts_ms" to millis))
        assertSame(heartbeat, transform.apply(heartbeat))
    }

    @ParameterizedTest
    @ValueSource(strings = ["c", "r", "u"])
    fun fillsSchemalessEventsWithoutMutatingInput(op: String) {
        val row = mapOf("id" to 42, "updated_at" to null)
        val envelope = mapOf("op" to op, "after" to row, "ts_ms" to millis)
        val original = SourceRecord(null, null, "products", null, envelope)
        val result = transform.apply(original)
        val output = (result.value() as Map<*, *>)["after"] as Map<*, *>
        assertEquals(if (op == "u") null else millis, output["created_at"])
        assertEquals(millis, output["updated_at"])
        assertEquals(42, output["id"])
        assertNull(row["updated_at"])
        assertNull(result.valueSchema())
    }

    @Test
    fun rejectsUnsupportedNullFieldTypesAndMalformedTimestamps() {
        assertFailsWith<DataException> {
            transform.apply(event("c", rowSchema(Schema.OPTIONAL_STRING_SCHEMA)))
        }
        val envelope = mapOf("op" to "c", "after" to emptyMap<String, Any>(), "ts_ms" to "yesterday")
        assertFailsWith<DataException> {
            transform.apply(SourceRecord(null, null, "products", null, envelope))
        }
        val wrongAfter = envelope + ("after" to "not a row")
        assertFailsWith<DataException> {
            transform.apply(SourceRecord(null, null, "products", null, wrongAfter))
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["c", "r", "u"])
    fun usesConfiguredFieldNamesInSchemasAndMaps(op: String) {
        val original = event(op)
        transform.apply(original)
        transform.configure(mapOf("created.field" to "createdOn", "updated.field" to "modifiedOn"))
        val output = (transform.apply(original).value() as Struct).getStruct("after")
        assertEquals(if (op == "u") null else Date(millis), output.get("createdOn"))
        assertEquals(Date(millis), output.get("modifiedOn"))
        assertNull(output.schema().field("created_at"))
        assertNull(output.schema().field("updated_at"))
        output.validate()

        val envelope = mapOf("op" to op, "ts_ms" to millis, "after" to mapOf("id" to 42))
        val schemaless = SourceRecord(null, null, "products", null, envelope)
        val row = (transform.apply(schemaless).value() as Map<*, *>)["after"] as Map<*, *>
        assertEquals(if (op == "u") null else millis, row["createdOn"])
        assertEquals(millis, row["modifiedOn"])
        assertTrue("created_at" !in row)
        assertTrue("updated_at" !in row)
    }

    @Test
    fun rejectsEmptyNullAndIdenticalFieldNames() {
        for (name in listOf("created.field", "updated.field")) {
            for (value in listOf(null, "", " ")) {
                assertFailsWith<ConfigException> { transform.configure(mapOf(name to value)) }
            }
        }
        assertFailsWith<ConfigException> {
            transform.configure(mapOf("created.field" to "timestamp", "updated.field" to "timestamp"))
        }
    }

    private fun rowSchema(timestamp: Schema? = null): Schema {
        val builder = SchemaBuilder.struct().name("products.Value").doc("Row").version(2)
            .parameter("source", "products").optional().field("id", Schema.INT32_SCHEMA)
        if (timestamp != null) builder.field("created_at", timestamp).field("updated_at", timestamp)
        return builder.build()
    }

    private fun event(op: String, row: Schema = rowSchema(), before: Boolean = false): SourceRecord {
        val schema = SchemaBuilder.struct().name("products.Envelope")
            .field("before", row).field("after", row).field("op", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .field("source", SchemaBuilder.struct().field("ts_ms", Schema.INT64_SCHEMA).build()).build()
        val value = Struct(schema).put("op", op).put("ts_ms", millis)
            .put("after", Struct(row).put("id", 42))
            .put("before", if (before) Struct(row).put("id", 41) else null)
            .put("source", Struct(schema.field("source").schema()).put("ts_ms", millis - 10_000))
        return SourceRecord(mapOf("db" to "test"), mapOf("lsn" to 123L), "products", 1,
            Schema.INT32_SCHEMA, 42, schema, value, millis - 1_000).apply { headers().addString("test", "value") }
    }
}
