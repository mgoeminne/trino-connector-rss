package com.goeminne.trino.rss;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

/**
 * A Trino Plugin that provides a connector for RSS sources.
 */
public class RSSPlugin implements Plugin {
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return ImmutableList.of(new RSSConnectorFactory());
    }
}
