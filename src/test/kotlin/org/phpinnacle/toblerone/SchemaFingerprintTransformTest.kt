package org.phpinnacle.toblerone

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

internal class SchemaFingerprintTransformTest {
    @ParameterizedTest
    @ValueSource(strings = ["Envelope", "Value"])
    fun preservesSchemaNamesAndRecordMetadata(suffix: String) {
        val schema = SchemaBuilder.struct().name("inventory.public.customers.$suffix")
            .version(2).doc("Customers").parameter("origin", "database").optional()
            .field("id", Schema.INT64_SCHEMA).build()
        val input = record(schema, Struct(schema).put("id", 42L))
        input.headers().addString("source", "test")

        transformation().use { transform ->
            val result = transform.apply(input)

            assertTrue(result.valueSchema().name().matches(
                Regex("toblerone\\.fingerprint\\.p_[0-9a-f]{64}\\.inventory\\.public\\.customers\\.$suffix")
            ))
            assertEquals(schema.version(), result.valueSchema().version())
            assertEquals(schema.doc(), result.valueSchema().doc())
            assertEquals(schema.parameters(), result.valueSchema().parameters())
            assertEquals(schema.isOptional, result.valueSchema().isOptional)
            assertEquals(42L, (result.value() as Struct).getInt64("id"))
            assertSame(result.valueSchema(), (result.value() as Struct).schema())
            assertSame(input.keySchema(), result.keySchema())
            assertSame(input.key(), result.key())
            assertEquals(input.sourcePartition(), result.sourcePartition())
            assertEquals(input.sourceOffset(), result.sourceOffset())
            assertEquals(input.topic(), result.topic())
            assertEquals(input.kafkaPartition(), result.kafkaPartition())
            assertEquals(input.timestamp(), result.timestamp())
            assertEquals(input.headers(), result.headers())
        }
    }

    @Test
    fun fingerprintsNestedSchemaChangesIndependentlyOfRecordValues() {
        val narrow = row("id" to Schema.INT64_SCHEMA)
        val wide = row("id" to Schema.INT64_SCHEMA, "name" to Schema.OPTIONAL_STRING_SCHEMA)
        val changedType = row("id" to Schema.STRING_SCHEMA)

        transformation().use { transform ->
            val original = transform.apply(envelope(narrow, Struct(narrow).put("id", 1L)))
            val another = transform.apply(envelope(narrow, Struct(narrow).put("id", 2L)))
            val expanded = transform.apply(envelope(wide, Struct(wide).put("id", 1L).put("name", "Alice")))
            val changed = transform.apply(envelope(changedType, Struct(changedType).put("id", "1")))

            assertEquals(original.valueSchema().name(), another.valueSchema().name())
            assertNotEquals(original.valueSchema().name(), expanded.valueSchema().name())
            assertNotEquals(original.valueSchema().name(), changed.valueSchema().name())
            assertEquals(original.valueSchema().name(), transform.apply(
                envelope(row("id" to Schema.INT64_SCHEMA), Struct(narrow).put("id", 3L))
            ).valueSchema().name())
            assertSame(narrow, original.valueSchema().field("before").schema())
            assertSame(wide, expanded.valueSchema().field("after").schema())
        }
    }

    @Test
    fun canonicalizesParameterMapsWithoutReorderingRecordFields() {
        fun schema(parameters: Map<String, String>, reversed: Boolean = false): Schema {
            val fields = listOf("id" to Schema.INT64_SCHEMA, "name" to Schema.OPTIONAL_STRING_SCHEMA)
            val builder = SchemaBuilder.struct().name("Customer").parameters(parameters)
            (if (reversed) fields.reversed() else fields).forEach { builder.field(it.first, it.second) }
            return builder.build()
        }
        val first = schema(linkedMapOf("a" to "1", "b" to "2"))
        val second = schema(linkedMapOf("b" to "2", "a" to "1"))
        val reordered = schema(first.parameters(), reversed = true)

        fun fingerprint(schema: Schema): String = transformation().use { transform ->
            transform.apply(record(schema, Struct(schema).put("id", 1L))).valueSchema().name()
        }

        assertEquals(fingerprint(first), fingerprint(second))
        assertNotEquals(fingerprint(first), fingerprint(reordered))
    }

    @Test
    fun includesDefaultsAndOptionalityWithoutReplacingExplicitNulls() {
        val withDefault = row("id" to SchemaBuilder.int64().optional().defaultValue(42L).build())
        val withoutDefault = row("id" to Schema.OPTIONAL_INT64_SCHEMA)
        val required = row("id" to Schema.INT64_SCHEMA)

        transformation().use { transform ->
            val result = transform.apply(record(withDefault, Struct(withDefault).put("id", null)))
            assertNull((result.value() as Struct).getWithoutDefault("id"))
            assertEquals(42L, result.valueSchema().field("id").schema().defaultValue())
            assertNotEquals(result.valueSchema().name(),
                transform.apply(record(withoutDefault, Struct(withoutDefault))).valueSchema().name())
            assertNotEquals(
                transform.apply(record(withoutDefault, Struct(withoutDefault))).valueSchema().name(),
                transform.apply(record(required, Struct(required).put("id", 42L))).valueSchema().name()
            )
        }
    }

    @Test
    fun distinguishesExplicitNullsInArrayDefaults() {
        fun schema(defaultId: Long?): Schema {
            val item = SchemaBuilder.int64().optional().defaultValue(42L).build()
            return row("ids" to SchemaBuilder.array(item).defaultValue(listOf(defaultId)).build())
        }
        val withNull = schema(null)
        val withValue = schema(42L)

        transformation().use { transform ->
            assertNotEquals(
                transform.apply(record(withNull, Struct(withNull).put("ids", emptyList<Long>()))).valueSchema().name(),
                transform.apply(record(withValue, Struct(withValue).put("ids", emptyList<Long>()))).valueSchema().name()
            )
        }
    }

    @Test
    fun leavesTombstonesAndNonRecordValuesUnchanged() {
        val schema = row("id" to Schema.INT64_SCHEMA)
        val records = listOf(
            record(null, null), record(schema, null), record(null, mapOf("id" to 1)),
            record(Schema.STRING_SCHEMA, "value")
        )

        transformation().use { transform ->
            records.forEach { assertSame(it, transform.apply(it)) }
        }
    }

    private fun transformation(): SchemaFingerprintTransform<SourceRecord> =
        SchemaFingerprintTransform<SourceRecord>().apply { configure(emptyMap<String?, Any>()) }

    private fun row(vararg fields: Pair<String, Schema>): Schema {
        val builder = SchemaBuilder.struct().name("inventory.public.customers.Value").optional()
        fields.forEach { builder.field(it.first, it.second) }
        return builder.build()
    }

    private fun envelope(row: Schema, value: Struct): SourceRecord {
        val schema = SchemaBuilder.struct().name("inventory.public.customers.Envelope")
            .field("before", row).field("after", row).field("op", Schema.STRING_SCHEMA).build()
        return record(schema, Struct(schema).put("before", null).put("after", value).put("op", "c"))
    }

    private fun record(schema: Schema?, value: Any?): SourceRecord = SourceRecord(
        mapOf("server" to "inventory"), mapOf("lsn" to 42L), "inventory.public.customers", 2,
        Schema.INT64_SCHEMA, 42L, schema, value, 1000L
    )
}
