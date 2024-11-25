package com.goeminne.trino.rss;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.List;

/**
 * A Trino Plugin that provides a connector for RSS sources.
 */
public class RSSPlugin implements Plugin {
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return List.of(new RSSConnectorFactory());
    }
}
