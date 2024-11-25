package com.goeminne.trino.rss;

import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class RSSConnectorFactory implements ConnectorFactory {

    @Override
    public String getName() { return "rss"; }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        requireNonNull(config, "requiredConfig is null");
        checkStrictSpiVersionMatch(context, this);

        List<URI> uris = Arrays.stream(config.get("rss.uris")
            .split(","))
            .map(entry -> {
                try {
                    return new URI(entry);
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            })
            .toList();

        RSSConfig rssConfig = new RSSConfig();
        rssConfig.setURIs(uris);
        RSSClient client = new RSSClient(rssConfig);

        return new RSSConnector(
            new LifeCycleManager(List.of(), null),
            new RSSSplitManager(client),
            new RSSRecordSetProvider()
        );
    }
}
