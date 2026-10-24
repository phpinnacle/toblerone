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

internal class SlugTransformTest {
    private val xformKey: SlugTransform<SourceRecord> = SlugTransform.Key()
    private val xformValue: SlugTransform<SourceRecord> = SlugTransform.Value()

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
        configure(xformValue)

        val valueSchema = SchemaBuilder
            .struct()
            .name("name")
            .version(1)
            .doc("doc")
            .field("name", Schema.STRING_SCHEMA)
            .field("string", Schema.STRING_SCHEMA)
            .build()

        val value = Struct(valueSchema)
            .put("name", "Супер пупер товар")
            .put("string", "test")

        val original = SourceRecord(null, null, "test", 0, valueSchema, value)
        val transformed: SourceRecord = xformValue.apply(original)
        val transformedSchema = transformed.valueSchema()
        val transformedValue = transformed.value() as Struct

        assertEquals(valueSchema.name(), transformedSchema.name())
        assertEquals(valueSchema.version(), transformedSchema.version())
        assertEquals(valueSchema.doc(), transformedSchema.doc())

        assertEquals(Schema.STRING_SCHEMA, transformedSchema.field("name").schema())
        assertEquals("Супер пупер товар", transformedValue.get("name"))

        assertEquals(Schema.STRING_SCHEMA, transformedSchema.field("code").schema())
        assertEquals("super-puper-tovar", transformedValue.get("code"))

        assertEquals(Schema.STRING_SCHEMA, transformedSchema.field("string").schema())
        assertEquals("test", transformedValue.getString("string"))
    }

    @Test
    fun schemalessValueConvertField() {
        configure(xformValue, separators = ".")
        val original = mapOf(
            "int32" to 42,
            "name" to "Супер пупер товар 2.0"
        )

        val record = SourceRecord(null, null, "test", 0, null, original)
        val transformed = xformValue.apply(record).value() as Map<*, *>

        assertEquals(42, transformed["int32"])
        assertEquals("Супер пупер товар 2.0", transformed["name"])
        assertEquals("super-puper-tovar-2-0", transformed["code"])
    }

    @Test
    fun passUnknownSchemaFields() {
        configure(xformValue)
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

    private fun configure(
        transform: SlugTransform<SourceRecord>,
        input: String = "name",
        output: String = "code",
        separators: String = "",
    ) {
        val props: MutableMap<String, String> = HashMap()

        props["input"] = input
        props["output"] = output
        props["separators"] = separators

        transform.configure(props.toMap())
    }
}
