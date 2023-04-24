package org.phpinnacle.toblerone

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import java.util.*
import kotlin.test.Test
import kotlin.test.assertNull

internal class RadixTransformTest {
    private val xformKey: RadixTransform<SourceRecord> = RadixTransform.Key()
    private val xformValue: RadixTransform<SourceRecord> = RadixTransform.Value()

    @AfterEach
    fun teardown() {
        xformKey.close()
        xformValue.close()
    }

    @Test
    fun handlesNullValue() {
        configure(xformValue)
        val given = SourceRecord(
            null,
            null,
            "topic",
            0,
            null,
            null
        )
        val expected = null
        val actual: Any? = xformValue.apply(given).value()
        assertEquals(expected, actual)
    }

    @Test
    fun handlesNullKey() {
        configure(xformKey)
        val given = SourceRecord(
            null,
            null,
            "topic",
            0,
            null,
            null,
            null,
            null
        )
        val expected = null
        val actual: Any? = xformKey.apply(given).key()
        assertEquals(expected, actual)
    }

    @Test
    fun copyValueSchemaAndConvertFields() {
        configure(xformValue, "id:36")

        val schema = SchemaBuilder
            .struct()
            .name("name")
            .version(1)
            .doc("doc")
            .field("id", Schema.STRING_SCHEMA)
            .field("int32", Schema.INT32_SCHEMA)
            .build()

        val expected = Struct(schema)
            .put("id", "  PJV  ")
            .put("int32", 42)

        val original = SourceRecord(null, null, "test", 0, schema, expected)
        val transformed: SourceRecord = xformValue.apply(original)

        assertEquals(schema.name(), transformed.valueSchema().name())
        assertEquals(schema.version(), transformed.valueSchema().version())
        assertEquals(schema.doc(), transformed.valueSchema().doc())

        assertEquals(Schema.INT32_SCHEMA, transformed.valueSchema().field("id").schema())
        assertEquals(33115, (transformed.value() as Struct).getInt32("id"))

        assertEquals(Schema.INT32_SCHEMA, transformed.valueSchema().field("int32").schema())
        assertEquals(42, (transformed.value() as Struct).getInt32("int32"))
    }

    @Test
    fun schemalessValueConvertField() {
        configure(xformValue, "id:36")
        val original = mapOf(
            "id" to "   PJV   ",
            "int32" to 42,
        )

        val record = SourceRecord(null, null, "test", 0, null, original)
        val transformed = xformValue.apply(record).value() as Map<*, *>

        assertEquals(33115, transformed["id"])
        assertEquals(42, transformed["int32"])
    }

    @Test
    fun passUnknownSchemaFields() {
        configure(xformValue, "string")
        val schema = SchemaBuilder
            .struct()
            .name("name")
            .version(1)
            .doc("doc")
            .field("int32", Schema.INT32_SCHEMA)
            .build()

        val expected = Struct(schema).put("int32", 42)
        val original = SourceRecord(null, null, "test", 0, schema, expected)
        val transformed: SourceRecord = xformValue.apply(original)

        assertEquals(schema.name(), transformed.valueSchema().name())
        assertEquals(schema.version(), transformed.valueSchema().version())
        assertEquals(schema.doc(), transformed.valueSchema().doc())
        assertEquals(Schema.INT32_SCHEMA, transformed.valueSchema().field("int32").schema())
        assertEquals(42, (transformed.value() as Struct).getInt32("int32"))
    }

    @Test
    fun topLevelStructRequired() {
        configure(xformValue)
        assertThrows(DataException::class.java) {
            xformValue.apply(
                SourceRecord(
                    null, null,
                    "topic", 0, Schema.INT32_SCHEMA, 42
                )
            )
        }
    }

    @Test
    fun topLevelMapRequired() {
        configure(xformValue)
        assertThrows(DataException::class.java) {
            xformValue.apply(
                SourceRecord(
                    null, null,
                    "topic", 0, null, 42
                )
            )
        }
    }

    @Test
    fun testOptionalStruct() {
        configure(xformValue)
        val builder = SchemaBuilder.struct().optional()
        builder.field("opt_int32", Schema.OPTIONAL_INT32_SCHEMA)
        val schema = builder.build()
        val transformed: SourceRecord = xformValue.apply(
            SourceRecord(
                null, null,
                "topic", 0,
                schema, null
            )
        )
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type())
        assertNull(transformed.value())
    }

    private fun configure(transform: RadixTransform<SourceRecord>, fields: String = "") {
        val props: MutableMap<String, String> = HashMap()

        props["fields"] = fields

        transform.configure(props.toMap())
    }
}
