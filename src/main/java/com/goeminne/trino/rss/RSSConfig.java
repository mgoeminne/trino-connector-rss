package com.goeminne.trino.rss;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

import java.net.URI;

public class RSSConfig {
    private URI metadata;

    @NotNull
    public URI getMetadata() {
        return metadata;
    }

    @Config("metadata-uri")
    public RSSConfig setMetadata(URI metadata) {
        this.metadata = metadata;
        return this;
    }
}
