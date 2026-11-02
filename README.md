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

Kafka (including `connect-json`), Debezium, PostgreSQL JDBC, Jackson and SLF4J classes are supplied by the host and excluded
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
| `BooleanToStringTransform$Value` | `fields`, `true`, `false` | Replaces selected boolean fields with the configured strings. |
| `StringMapTransform$Value` | `field`, `mapping.<source>` | Replaces exact strings in one field using the configured mappings. |
| `EmptyStringToNullTransform$Value` | `fields`, optional `trim` | Replaces empty strings in selected fields with null. |
| `SlugTransform$Value` | `input`, `output`, optional `separators` | Transliterates the input to Latin and inserts a lowercase slug with hyphens. |
| `JsonTransform$Value` | optional `fields` | Serializes selected fields to JSON strings, or the entire value when `fields` is omitted or empty. |
| `MoneyTransform$Value` | `fields`, `currency`, optional `mode` | Converts Decimal or FLOAT64 amounts to INT64 currency minor units. |

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

For example, convert `is_active` to `"yes"` or `"no"`:

```json
{
  "transforms": "booleanToString",
  "transforms.booleanToString.type": "org.phpinnacle.toblerone.BooleanToStringTransform$Value",
  "transforms.booleanToString.fields": "is_active",
  "transforms.booleanToString.true": "yes",
  "transforms.booleanToString.false": "no"
}
```

Both `true` and `false` settings are required; empty strings are allowed. Use a comma-separated
`fields` list to convert multiple fields. Nulls, missing fields and non-boolean values are left
unchanged. With a schema, converted fields become strings and retain their optional status and
mapped default values. Like `ToggleTransform`, this transformation works on fields in a `Struct`
or schemaless map. Use `$Key` to transform key fields.

To map strings in a single field:

```json
{
  "transforms": "map",
  "transforms.map.type": "org.phpinnacle.toblerone.StringMapTransform$Value",
  "transforms.map.field": "status",
  "transforms.map.mapping.draft": "черновик",
  "transforms.map.mapping.published": "опубликовано",
  "transforms.map.mapping.deleted": ""
}
```

`field` and at least one `mapping.<source>` setting are required. Each mapping value must be a
string; empty strings are allowed as sources (`mapping.`) and replacements. Matching uses the
entire string, is case-sensitive and preserves whitespace. Replacements are applied once.
Unmapped strings, nulls, non-string values and missing fields are left unchanged. The original
schema is retained. Use `$Key` to transform key fields.

`CONFIG_DEF` declares `field`; dynamic `mapping.*` properties are read and validated in
`configure()`. Kafka Connect passes these properties to the transformation, but its configuration
schema does not list the individual mappings. Invalid mappings fail when the transformation
is configured.

To replace empty strings in selected top-level fields with null:

```json
{
  "transforms": "emptyToNull",
  "transforms.emptyToNull.type": "org.phpinnacle.toblerone.EmptyStringToNullTransform$Value",
  "transforms.emptyToNull.fields": "name,description"
}
```

`fields` is a required nonempty comma-separated list. By default, only the exact empty string (`""`) is
converted; whitespace, nonempty strings, nulls, non-string values and missing fields are unchanged.
Set `transforms.emptyToNull.trim=true` to trim leading and trailing whitespace first: `"   "`
becomes null and `"  hello world  "` becomes `"hello world"`. Whitespace inside strings is retained.
`trim` defaults to `false` and only affects selected fields.
Both schemaless maps and schema-based Structs are supported; use `$Key` to transform key fields.
Selected string schemas become optional and lose their defaults so that a resulting null is not
replaced by a default. Required input fields still resolve their input defaults before conversion;
optional input fields preserve explicit nulls. Other field schemas and schema metadata are retained.
For Debezium events, apply this transform after `ExtractNewRecordState` to select row fields.

To serialize selected top-level fields to JSON strings:

```json
{
  "transforms": "json",
  "transforms.json.type": "org.phpinnacle.toblerone.JsonTransform$Value",
  "transforms.json.fields": "payload,metadata"
}
```

Omit `transforms.json.fields` or set it to an empty string to serialize the entire value.
Use `JsonTransform$Key` to serialize the key instead. Both schema and schemaless records are
supported. Selecting fields requires a Struct or map; whole-value mode also accepts scalars,
arrays and maps. Missing selected fields are ignored, and other fields are unchanged.

The result is a JSON string without a `schema`/`payload` envelope. For example, a field containing
`{"id":42}` becomes the string `"{\"id\":42}"`; a string containing `hello` becomes the JSON
string `"\"hello\""`. With a schema, serialized values receive a string schema; optional status
and JSON-encoded defaults are retained for converted fields. Struct metadata and unselected
field schemas are preserved. Null fields and null keys/values remain null, preserving tombstones.
Nested values and logical types use Kafka Connect's standard JSON encoding, including Base64
for bytes and the default Decimal encoding. The host must provide Kafka Connect's `connect-json`
module, as a standard Kafka Connect installation does.

### Money in minor units

```json
{
  "transforms": "money",
  "transforms.money.type": "org.phpinnacle.toblerone.MoneyTransform$Value",
  "transforms.money.mode": "minor",
  "transforms.money.fields": "amount,balance",
  "transforms.money.currency": "RUB"
}
```

`fields` is a required nonempty list of top-level fields. `currency` is a required, supported
three uppercase letter ISO currency code. `mode` defaults to `minor`, currently its only accepted
value. Use `$Key` to transform key fields. For Debezium events, apply this transform after
`ExtractNewRecordState`, when the selected fields are at the top level.

Input fields must be Kafka Connect `Decimal` (`BigDecimal`) or `FLOAT64` (`Double`), as produced
for PostgreSQL `NUMERIC` with Debezium's `decimal.handling.mode=precise` or `double`, respectively.
Schemaless maps containing `BigDecimal` or `Double` are also supported; strings, integers,
`Float` values and raw bytes are rejected. Double values use their canonical decimal string
representation (`BigDecimal.valueOf`): BYN `0.29` becomes `29`. Precision already lost upstream
cannot be recovered; use `precise` when the original decimal precision must be retained.
Conversion uses ICU4J's standard currency fraction digits, without cash rounding:
RUB `1.2300` becomes `123`, JPY `123.0000` becomes `123`, and KWD `1.2340` becomes `1234`.

Selected fields are replaced with `Long` values and `INT64` schemas. Nulls and missing fields
are preserved, as are unselected fields and record metadata. Field optionality and converted
defaults are retained; the Decimal logical name and its scale parameters are removed.
Fractional minor units (e.g. RUB `1.2340` or a Double result of `0.1 + 0.2`), NaN, infinity,
INT64 overflow, incompatible field schemas and unconvertible defaults fail with `DataException`;
amounts are never rounded or truncated.
Invalid configuration fails with `ConfigException`. Currency fraction digits come from the
bundled ICU data, so review changes to those rules when upgrading ICU.

### Debezium timestamp fallback

`DebeziumTimestampsTransform` fills missing or null `created_at` and `updated_at` fields in
SQL Debezium events. Configure the field names with `created.field` and `updated.field`
(defaults: `created_at` and `updated_at`); both must be nonempty and distinct. Place it **before**
`ExtractNewRecordState`, while the envelope still contains the operation and the `after` row:

```json
{
  "transforms": "timestamps,unwrap",
  "transforms.timestamps.type": "org.phpinnacle.toblerone.DebeziumTimestampsTransform",
  "transforms.timestamps.created.field": "created_at",
  "transforms.timestamps.updated.field": "updated_at",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
}
```

| Debezium operation | Fallback for `created_at` | Fallback for `updated_at` |
| --- | --- | --- |
| Create (`c`) or snapshot read (`r`) | Event processing time | Event processing time |
| Update (`u`) | No fallback | Event processing time |
| Delete (`d`), tombstone, other events | Unchanged | Unchanged |

Existing non-null values always win, including on updates. The transform does not store state:
on update it cannot recover an absent creation time. Newly added schema fields are optional
Kafka Connect `Timestamp` values; `created_at` stays null on updates when no value is available.
Snapshot fallback represents the snapshot processing time, not the original row creation time.

The fallback uses the envelope's top-level `ts_ms`, which Debezium defines as the connector's
event processing time, rather than `source.ts_ms` (database change time). If `ts_ms` is null
or absent, it uses the Connect worker's current clock. This is an approximation of ingestion
time: a source SMT runs before publication and cannot know the broker's append time.
See the [Debezium PostgreSQL event documentation](https://debezium.io/documentation/reference/3.0/connectors/postgresql.html#postgresql-create-events).

For existing schema fields, fallback preserves Kafka Connect `Timestamp`, Debezium `Timestamp`
(milliseconds), `MicroTimestamp` (microseconds), and `ZonedTimestamp` (UTC ISO-8601 string).
Filling a null field of any other type fails with `DataException`, since its time format is unknown.
Schemaless maps are also supported; their generated timestamps are epoch milliseconds.
Keys, record metadata, tombstones and non-row events are preserved. With schemas, `before`
and `after` retain a matching row schema; any new fields in `before` remain null.

### Debezium converters

These are Debezium custom converters, configured through `converters`, rather than SMTs.
They use Debezium API 3.0.7.Final and require Java 17 or later at runtime.

| Class in `org.phpinnacle.toblerone` | Configuration | Behavior |
| --- | --- | --- |
| `MCharConverter` | None | Converts PostgreSQL `mchar` and `mvarchar` values to UTF-8 strings. |
| `IntegerConverter` | `columns` | Reads the first four bytes as a big-endian 32-bit integer. |
| `UuidConverter` | `columns`, optional `zero.as.null` | Converts a binary UUID using the byte ordering of the source 1C data. |

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

To convert zero UUIDs to null, enable `zero.as.null` for the UUID converter:

```json
{
  "uuid.zero.as.null": "true"
}
```

The setting defaults to `false`, preserving existing behavior. When enabled, decoded
`00000000-0000-0000-0000-000000000000` values become null for all selected `uuid.columns`;
other UUIDs and existing nulls are unchanged. Selected column schemas become optional even
when the source columns are `NOT NULL`. Unselected columns are unaffected.
