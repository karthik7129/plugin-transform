package io.kestra.plugin.transform.arrow;

public enum OutputFormat {
    /** Amazon Ion records, one value per jq output. Kestra's internal format. */
    ION,
    /** One JSON value per line. */
    NDJSON
}
