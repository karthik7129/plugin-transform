# How to use the Transform Arrow plugin

Query Parquet, Arrow IPC, CSV, NDJSON, and Ion files with a [jq](https://jqlang.org/) expression. This is the same idea as [aq](https://github.com/Anaethelion/aq): read a columnar file, turn each row into a JSON object, and filter or reshape it with jq.

## Tasks

`Query` reads `from` (a `kestra://` internal storage URI) and writes every jq output as a record. The default output is Amazon Ion, so the result can be passed to other Kestra tasks. Set `outputFormat: NDJSON` to write one JSON value per line instead.

Give `expression` a jq 1.6 filter. It is compiled once. An invalid expression fails the task before any row is read. If a row makes the expression fail at runtime, the task fails and the message includes the zero-based row index.

By default the expression runs once per row. jq decides how many records that produces: `select(.total > 100)` writes nothing when the row does not match, `.tags` writes the array as one record, and `.tags[]` writes one record per element. A JSON null output is dropped, because an Ion file cannot store a null row.

Set `slurp: true` to collect every row into one JSON array and evaluate the expression once, the same as `jq -s` or `aq -s`. Use it for aggregates such as `[.[].total] | add / length`. Slurp keeps the whole file in memory.

## Input format

`inputFormat` defaults to `AUTO`.

1. The file extension is used when it is one of `.parquet`, `.pq`, `.arrow`, `.arrows`, `.ipc`, `.feather`, `.csv`, `.tsv`, `.ndjson`, `.jsonl`, `.json`, or `.ion`.
2. Otherwise the leading bytes are checked for Parquet (`PAR1`), an Arrow IPC file (`ARROW1`), an Arrow IPC stream (the `0xFFFFFFFF` continuation marker), or binary Ion.

Set `inputFormat` when the name has no extension and the bytes are ambiguous. Arrow IPC streams that do not start with the continuation marker need `inputFormat: ARROW`.

Parquet and Arrow IPC files are staged in the working directory because those formats need random access. Arrow is still read one record batch at a time, so memory stays bounded by the batch size unless `slurp` is set.

CSV options live under `csvOptions`: `delimiter` (default `,`), `header` (default `true`), `charset` (default `UTF-8`), and `inferTypes` (default `true`). With no header, fields are named `column_0`, `column_1`, and so on. Type inference turns blanks into null and recognizes booleans, integers, and decimals. Turn it off to keep every field a string. A `.tsv` file is read as CSV; set `delimiter` to a tab.

## Values

Rows become JSON objects before jq runs.

| Source | JSON |
| --- | --- |
| Integers and floats | number |
| Unsigned 64-bit integer | number, full precision (not a signed long) |
| Decimal | number |
| String | string |
| Binary | base64 string |
| Boolean | boolean |
| List | array |
| Struct | object |
| Map | array of `{"key", "value"}` |
| Dictionary | the decoded value |
| Date, time, timestamp | ISO-8601 string |
| Null | null |

The same expression therefore sees the same shapes from Parquet, Arrow, CSV, NDJSON, and Ion, aside from types the format itself cannot represent.

## jq compatibility

The engine is [jackson-jq](https://github.com/eiiches/jackson-jq) targeting jq 1.6, not the C jq binary. `$ENV`, `input` / `inputs`, and some path builtins are missing. That is the same kind of gap `aq` accepts by using `jaq`.
