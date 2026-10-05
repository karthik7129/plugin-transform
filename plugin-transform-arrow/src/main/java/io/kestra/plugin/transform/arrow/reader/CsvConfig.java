package io.kestra.plugin.transform.arrow.reader;

import java.nio.charset.Charset;

public record CsvConfig(char delimiter, boolean header, Charset charset, boolean inferTypes) {
}
