package org.phpinnacle.toblerone

import io.debezium.spi.converter.CustomConverter
import io.debezium.spi.converter.RelationalColumn
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.transforms.Transformation
import org.postgresql.util.PGobject
import java.io.File
import java.util.OptionalInt
import java.util.Properties
import java.util.ServiceLoader
import java.util.zip.ZipFile
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertSame

internal class PluginPackagingTest {
    private val pluginJar = File(System.getProperty("plugin.jar"))

    @Test
    fun doesNotBundleClassesProvidedByConnectAndDebezium() {
        val providedPackages = listOf(
            "org/apache/kafka/", "io/debezium/", "org/postgresql/", "org/slf4j/", "com/fasterxml/jackson/"
        )
        ZipFile(pluginJar).use { jar ->
            assertFalse(jar.entries().asSequence().any { entry ->
                providedPackages.any { entry.name.startsWith(it) }
            })
        }
    }

    @Test
    fun discoversAllTransformationsWithKafkaClassLoader() {
        classLoader().use { loader ->
            val transformations = ServiceLoader.load(Transformation::class.java, loader).toList()
                .filter { it.javaClass.name.startsWith("org.phpinnacle.toblerone.") }
            val expected = listOf("TrimTransform", "SubstringTransform", "RadixTransform",
                "InsertUuid", "ToggleTransform", "SlugTransform")
                .flatMap { name -> listOf("$name\$Key", "$name\$Value") }
                .map { "org.phpinnacle.toblerone.$it" }.toSet()

            assertEquals(expected, transformations.map { it.javaClass.name }.toSet())
            transformations.forEach { assertSame(loader, it.javaClass.classLoader) }
        }
    }

    @Test
    fun transformsRecordsFromPackagedJar() {
        classLoader().use { loader ->
            assertEquals(mapOf("name" to "value"), apply(loader, "TrimTransform", mapOf("fields" to "name"),
                mapOf("name" to " value ")))
            assertEquals(mapOf("name" to null), apply(loader, "SubstringTransform", mapOf("fields" to "name:0:3"),
                mapOf("name" to null)))
            assertEquals(mapOf("id" to "16"), apply(loader, "RadixTransform", mapOf("fields" to "id:36"),
                mapOf("id" to 42)))
            assertEquals(mapOf("active" to true), apply(loader, "ToggleTransform", mapOf("fields" to "active"),
                mapOf("active" to false)))
            val slug = apply(loader, "SlugTransform", mapOf("input" to "name", "output" to "slug"),
                linkedMapOf("name" to "Супер пупер товар"))
            assertEquals("super-puper-tovar", slug["slug"])
            val uuid = apply(loader, "InsertUuid", mapOf("fields" to "name,id", "output" to "uuid"),
                linkedMapOf("name" to "product", "id" to 42))
            assertEquals("d985c19d-45df-5445-b285-2ad3a5c233fb", uuid["uuid"])
        }
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun loadsConvertersAgainstProvidedDebeziumAndPostgresClasses() {
        val column = object : RelationalColumn {
            override fun name() = "value"
            override fun dataCollection() = "public.products"
            override fun typeName() = "mvarchar"
            override fun typeExpression() = "mvarchar"
            override fun scale(): OptionalInt = OptionalInt.empty()
            override fun nativeType() = 0
            override fun length(): OptionalInt = OptionalInt.empty()
            override fun jdbcType() = 0
            override fun isOptional() = true
            override fun hasDefaultValue() = false
            override fun defaultValue(): Any? = null
        }
        val inputs = mapOf(
            "MCharConverter" to (PGobject().apply { value = "Евро" } to "Евро"),
            "IntegerConverter" to (byteArrayOf(0, 0, 2, 0) to 512),
            "UuidConverter" to (ByteArray(16) to "00000000-0000-0000-0000-000000000000")
        )
        classLoader().use { loader ->
            for ((name, values) in inputs) {
                val converter = loader.loadClass("org.phpinnacle.toblerone.$name").getDeclaredConstructor()
                    .newInstance() as CustomConverter<SchemaBuilder, RelationalColumn>
                assertSame(loader, converter.javaClass.classLoader)
                converter.configure(Properties().apply { setProperty("columns", "public.products.value") })
                var conversion: CustomConverter.Converter? = null
                converter.converterFor(column) { _, handler -> conversion = handler }
                assertEquals(values.second, assertNotNull(conversion).convert(values.first))
            }
        }
    }

    private fun classLoader(): PluginClassLoader {
        val url = pluginJar.toURI().toURL()
        return PluginClassLoader(url, arrayOf(url), javaClass.classLoader)
    }

    @Suppress("UNCHECKED_CAST")
    private fun apply(
        loader: ClassLoader,
        name: String,
        config: Map<String, String>,
        value: Map<String, Any?>
    ): Map<*, *> {
        val transformation = loader.loadClass("org.phpinnacle.toblerone.$name\$Value").getDeclaredConstructor()
            .newInstance() as Transformation<SourceRecord>
        transformation.use {
            it.configure(config)
            return it.apply(SourceRecord(null, null, "test", 0, null, value)).value() as Map<*, *>
        }
    }
}
