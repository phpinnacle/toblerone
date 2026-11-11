package org.phpinnacle.toblerone

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.transforms.SetSchemaMetadata
import org.apache.kafka.connect.transforms.Transformation
import java.security.MessageDigest
import java.util.HexFormat

class SchemaFingerprintTransform<R : ConnectRecord<R>> : Transformation<R> {
    companion object {
        @Suppress("unused")
        const val OVERVIEW_DOC = "Prefix value record schema names with a fingerprint of their complete schema"

        val CONFIG_DEF = ConfigDef()
        private const val CACHE_SIZE = 64
    }

    private val converter = JsonConverter()
    private val mapper = ObjectMapper().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
    private lateinit var transforms: SynchronizedCache<Schema, SetSchemaMetadata.Value<R>>

    override fun configure(props: Map<String?, *>?) {
        converter.configure(mapOf("replace.null.with.default" to false), false)
        transforms = SynchronizedCache(LRUCache(CACHE_SIZE))
    }

    override fun apply(record: R): R {
        if (record.value() == null || record.valueSchema()?.type() != Schema.Type.STRUCT) return record

        val schema = record.valueSchema()
        val transform = transforms.get(schema) ?: metadata(schema).also { transforms.put(schema, it) }
        return transform.apply(record)
    }

    override fun close() = converter.close()
    override fun config(): ConfigDef = CONFIG_DEF

    private fun metadata(schema: Schema): SetSchemaMetadata.Value<R> {
        val definition = mapper.convertValue(converter.asJsonSchema(schema), Map::class.java)
        val fingerprint = HexFormat.of().formatHex(
            MessageDigest.getInstance("SHA-256").digest(mapper.writeValueAsBytes(definition))
        )

        return SetSchemaMetadata.Value<R>().apply {
            configure(mapOf(
                "schema.name" to "toblerone.fingerprint.p_$fingerprint.${schema.name() ?: "Value"}",
                "replace.null.with.default" to false
            ))
        }
    }
}
