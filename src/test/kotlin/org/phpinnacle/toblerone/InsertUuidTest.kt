package org.phpinnacle.toblerone

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.Assertions.assertThrows
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame

internal class InsertUuidTest {
    @Test
    fun generatesDeterministicUuidForSchemalessKeyAndValue() {
        val key = InsertUuid.Key<SourceRecord>()
        val value = InsertUuid.Value<SourceRecord>()
        val props = mapOf<String?, String>("fields" to "name,id", "output" to "uuid")
        key.configure(props)
        value.configure(props)

        val original = mapOf("name" to "product", "id" to 42)
        val record = SourceRecord(null, null, "test", 0, null, original, null, original)
        val expected = original + ("uuid" to "d985c19d-45df-5445-b285-2ad3a5c233fb")

        assertEquals(expected, key.apply(record).key())
        assertEquals(expected, value.apply(record).value())
        assertEquals(expected, value.apply(record).value())
    }

    @Test
    fun copiesSchemaAndGeneratesUuid() {
        val transform = InsertUuid.Value<SourceRecord>()
        transform.configure(mapOf("fields" to "name,id", "output" to "uuid"))
        val schema = SchemaBuilder.struct().name("product").version(1).doc("Product")
            .field("name", Schema.STRING_SCHEMA)
            .field("id", Schema.INT32_SCHEMA)
            .build()
        val original = Struct(schema).put("name", "product").put("id", 42)
        val record = SourceRecord(null, null, "test", 0, schema, original)

        val transformed = transform.apply(record)
        val result = transformed.value() as Struct

        assertEquals(schema.name(), transformed.valueSchema().name())
        assertEquals(schema.version(), transformed.valueSchema().version())
        assertEquals(schema.doc(), transformed.valueSchema().doc())
        assertEquals("product", result.getString("name"))
        assertEquals(42, result.getInt32("id"))
        assertEquals(Schema.STRING_SCHEMA, transformed.valueSchema().field("uuid").schema())
        assertEquals("d985c19d-45df-5445-b285-2ad3a5c233fb", result.getString("uuid"))
    }

    @Test
    fun usesConfiguredNamespace() {
        val transform = InsertUuid.Value<SourceRecord>()
        transform.configure(mapOf(
            "fields" to "name,id",
            "output" to "uuid",
            "namespace" to "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
        ))
        val original = mapOf("name" to "product", "id" to 42)
        val record = SourceRecord(null, null, "test", 0, null, original)

        val result = transform.apply(record).value() as Map<*, *>

        assertEquals("8bc9003e-72a9-5f45-85ca-b4ff6af4547a", result["uuid"])
    }

    @Test
    fun handlesNullAndMissingFields() {
        val transform = InsertUuid.Value<SourceRecord>()
        transform.configure(mapOf("fields" to "name,missing", "output" to "uuid"))
        val record = SourceRecord(null, null, "test", 0, null, mapOf("name" to null, "id" to 42))

        val result = transform.apply(record).value() as Map<*, *>

        assertEquals(5, UUID.fromString(result["uuid"] as String).version())
        assertEquals(null, result["name"])
        assertEquals(42, result["id"])
    }

    @Test
    fun preservesNullKeyAndValue() {
        val key = InsertUuid.Key<SourceRecord>()
        val value = InsertUuid.Value<SourceRecord>()
        val props = mapOf<String?, String>("fields" to "id", "output" to "uuid")
        key.configure(props)
        value.configure(props)
        val record = SourceRecord(null, null, "test", 0, null, null)

        assertSame(record, key.apply(record))
        assertSame(record, value.apply(record))
    }

    @Test
    fun rejectsInvalidNamespace() {
        assertThrows(ConfigException::class.java) {
            InsertUuid.Value<SourceRecord>().configure(mapOf(
                "fields" to "id", "output" to "uuid", "namespace" to "invalid"
            ))
        }
    }
}
