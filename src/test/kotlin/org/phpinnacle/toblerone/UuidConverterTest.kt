package org.phpinnacle.toblerone

import io.debezium.spi.converter.CustomConverter.Converter
import io.debezium.spi.converter.CustomConverter.ConverterRegistration
import io.debezium.spi.converter.RelationalColumn
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.SchemaBuilder
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.ByteBuffer
import java.util.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull

internal class UuidConverterTest {
    private class MockRegistration<S> : ConverterRegistration<S> {
        var converter: Converter? = null
        var schema: S? = null

        override fun register(fieldSchema: S, converter: Converter?) {
            this.converter = converter
            this.schema = fieldSchema
        }
    }

    @Test
    fun convertBytesToUuid() {
        val column = getColumn("test");
        val registration = MockRegistration<SchemaBuilder>()
        val converter = UuidConverter()
        val properties = Properties()

        properties.setProperty("columns", "public.table.test")

        converter.configure(properties)
        converter.converterFor(column, registration)

        val input = UUID.fromString("856f7cc2-5565-172c-11ef-3ce4edeaf984")
        val result = registration.converter?.convert(uuidToBytes(input))

        assertEquals("edeaf984-3ce4-11ef-856f-7cc25565172c", result)
    }

    @ParameterizedTest
    @ValueSource(strings = ["default", "false", "true"])
    fun convertsZeroUuidOnlyWhenEnabled(setting: String) {
        val enabled = setting == "true"
        for (optional in listOf(false, true)) {
            val converter = UuidConverter()
            val properties = Properties().apply {
                setProperty("columns", "public.table.test")
                if (setting != "default") setProperty("zero.as.null", setting)
            }
            converter.configure(properties)
            val registration = MockRegistration<SchemaBuilder>()
            converter.converterFor(getColumn("test", optional), registration)
            val conversion = assertNotNull(registration.converter)
            val schema = assertNotNull(registration.schema).build()

            assertEquals(optional || enabled, schema.isOptional)
            assertEquals("io.debezium.data.Uuid", schema.name())
            assertEquals(1, schema.version())
            assertEquals(if (enabled) null else "00000000-0000-0000-0000-000000000000",
                conversion.convert(ByteArray(16)))
            assertEquals("edeaf984-3ce4-11ef-856f-7cc25565172c",
                conversion.convert(uuidToBytes(UUID.fromString("856f7cc2-5565-172c-11ef-3ce4edeaf984"))))
            assertNull(conversion.convert(null))
            val unselected = MockRegistration<SchemaBuilder>()
            converter.converterFor(getColumn("other", optional), unselected)
            assertNull(unselected.converter)
        }
    }

    @Test
    fun rejectsInvalidZeroAsNullSetting() {
        val properties = Properties().apply { setProperty("zero.as.null", "yes") }
        assertFailsWith<ConfigException> { UuidConverter().configure(properties) }
    }

    private fun getColumn(name: String, optional: Boolean = false): RelationalColumn {
        return object : RelationalColumn {
            override fun typeName(): String {
                return "bytea"
            }

            override fun name(): String {
                return name
            }

            override fun dataCollection(): String {
                return "public.table"
            }

            override fun typeExpression(): String? {
                return null
            }

            override fun scale(): OptionalInt? {
                return null
            }

            override fun nativeType(): Int {
                return 0
            }

            override fun length(): OptionalInt? {
                return null
            }

            override fun jdbcType(): Int {
                return 0
            }

            override fun isOptional(): Boolean {
                return optional
            }

            override fun hasDefaultValue(): Boolean {
                return false
            }

            override fun defaultValue(): Any? {
                return null
            }
        }
    }

    private  fun uuidToBytes(uuid: UUID): ByteArray {
        val byteBuffer = ByteBuffer.allocate(16)
        byteBuffer.putLong(uuid.mostSignificantBits)
        byteBuffer.putLong(uuid.leastSignificantBits)
        return byteBuffer.array()
    }
}
