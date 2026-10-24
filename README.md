# Kafka Connect Single Message Transforms

This is an implementation of the [Kafka Connect SMT](https://docs.confluent.io/platform/current/connect/transforms/overview.html) interface 
to offer some widely used transformations

## Build

The library uses Gradle to build the JAR.

1. Install Java SDK 17 (required by the Debezium converters).
2. Checkout the Git repository and change to its root folder.
3. Execute `./gradlew build`

The plugin JAR is `build/libs/phpinnacle-toblerone-1.0.0-all.jar`.
The smaller JAR without `-all` is for Maven consumers and does not include dependencies.
Set `VERSION` to override the default version, for example `VERSION=0.1.0 ./gradlew build`.
Tagged releases use the tag without the `v` prefix as their version and publish the plugin as
`phpinnacle-toblerone.jar`, preserving the download filename used by Strimzi.

The build uses Gradle 8.1.1, Kotlin 1.9.25 and Java target 17. `./gradlew build` runs unit tests,
Detekt and packaging tests using Kafka Connect's plugin class loader.

## Runtime compatibility

The build targets the versions verified in the Alpaka cluster on 2026-10-24:

| Component | Runtime version |
| --- | --- |
| Java | 17.0.14 (Java 17 bytecode) |
| Kafka Connect | 3.9.0 |
| Debezium PostgreSQL | 3.0.7.Final |
| PostgreSQL JDBC | 42.6.1 |
| Jackson databind | 2.16.2 |

Kafka, Debezium, PostgreSQL JDBC, Jackson and SLF4J classes are supplied by the host and excluded
from the plugin JAR. Kotlin, UUID generation and ICU transliteration are bundled.

## Install

After the plugin JAR was built as described above, copy it to your Kafka Connect instance into one of the directories listed in
the `plugin.path` property in the connect worker configuration file.
> Make sure to do this on all Kafka Connect worker nodes!

See the [Confluent Kafka Connect Plugins Userguide](https://docs.confluent.io/home/connect/self-managed/userguide.html#installing-kconnect-plugins) for more details.

For the Debezium custom converters, place the plugin JAR in the same plugin directory as the
Debezium PostgreSQL connector so its API and PostgreSQL driver are visible to the converters.
In a Strimzi `KafkaConnect` build, add the JAR to the Debezium plugin's `artifacts` list.
The migrated classes use `org.phpinnacle.toblerone`; existing `org.alpaka.kafka` classes and
connector configurations remain unchanged until you explicitly migrate those configurations.

## Example

### Connector Configuration

```json
{
  "name": ...,
  "config": {
    ...,
    "transforms": "trim,substring,radix",
    "transforms.trim.type": "org.phpinnacle.toblerone.TrimTransform$Value",
    "transforms.trim.fields": "comma,separated,list,of,fields",
    "transforms.substring.type": "org.phpinnacle.toblerone.SubstringTransform$Value",
    "transforms.substring.fields": "foo:1:3,bar:2",
    "transforms.radix.type": "org.phpinnacle.toblerone.RadixTransform$Value",
    "transforms.radix.fields": "foo:16,bar:36"
  }
}
```

> Note that the transformer only supports some kind of structured input. So make sure that there is a [converter](https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/) class (e.g. [AvroConverter](https://www.confluent.io/hub/confluentinc/kafka-connect-avro-converter), [JsonConverter](https://www.confluent.io/hub/confluentinc/kafka-connect-json-schema-converter)) like `"value.converter": "io.confluent.connect.avro.AvroConverter"` that provides the data in a structured format.

### Additional transformations

These transformations support both schema-based `Struct` records and schemaless maps.
Use `$Key` instead of `$Value` to transform record keys.

| Class in `org.phpinnacle.toblerone` | Configuration | Behavior |
| --- | --- | --- |
| `InsertUuid$Value` | `fields`, `output`, optional `namespace` | Inserts a deterministic UUID v5 based on the selected fields; defaults to the URL namespace. |
| `ToggleTransform$Value` | `fields` | Inverts the selected boolean fields. |
| `SlugTransform$Value` | `input`, `output`, optional `separators` | Transliterates the input to Latin and inserts a lowercase slug with hyphens. |

```json
{
  "transforms": "uuid,toggle,slug",
  "transforms.uuid.type": "org.phpinnacle.toblerone.InsertUuid$Value",
  "transforms.uuid.fields": "id,name",
  "transforms.uuid.output": "uuid",
  "transforms.toggle.type": "org.phpinnacle.toblerone.ToggleTransform$Value",
  "transforms.toggle.fields": "is_active",
  "transforms.slug.type": "org.phpinnacle.toblerone.SlugTransform$Value",
  "transforms.slug.input": "name",
  "transforms.slug.output": "slug"
}
```

### Debezium converters

These are Debezium custom converters, configured through `converters`, rather than SMTs.
They use Debezium API 3.0.7.Final and require Java 17 or later at runtime.

| Class in `org.phpinnacle.toblerone` | Configuration | Behavior |
| --- | --- | --- |
| `MCharConverter` | None | Converts PostgreSQL `mchar` and `mvarchar` values to UTF-8 strings. |
| `IntegerConverter` | `columns` | Reads the first four bytes as a big-endian 32-bit integer. |
| `UuidConverter` | `columns` | Converts a binary UUID using the byte ordering of the source 1C data. |

`columns` is a comma-separated list of fully qualified column names (`schema.table.column`).
For example, add these properties to a Debezium connector configuration:

```json
{
  "converters": "mchar,integer,uuid",
  "mchar.type": "org.phpinnacle.toblerone.MCharConverter",
  "integer.type": "org.phpinnacle.toblerone.IntegerConverter",
  "integer.columns": "public.products.quantity",
  "uuid.type": "org.phpinnacle.toblerone.UuidConverter",
  "uuid.columns": "public.products.id"
}
```
