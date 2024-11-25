package com.goeminne.trino.rss;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class RSSConfig {
    private List<URI> uris;

    public RSSConfig() {
        this.uris = new ArrayList<>();
    }

    @NotNull
    public List<URI> getURIs() {
        return uris;
    }

    public void addURI(URI uri) {
        this.uris.add(uri);
    }

    @Config("rss.uris")
    public RSSConfig setURIs(List<URI> uris) {
        this.uris = uris;
        return this;
    }
}
