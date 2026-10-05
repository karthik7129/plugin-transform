package io.kestra.plugin.transform.arrow;

public enum InputFormat {
    /** Extension, then magic bytes. */
    AUTO,
    PARQUET,
    /** Arrow IPC file or stream. */
    ARROW,
    CSV,
    NDJSON,
    /** Kestra Ion records, text or binary. */
    ION
}
