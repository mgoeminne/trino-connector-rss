package com.goeminne.trino.rss;

import com.goeminne.trino.rss.channel.ChannelTable;
import com.goeminne.trino.rss.item.ItemTable;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class RSSClient {
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Map<String, Map<String, RSSTable>> schemas;

    public RSSClient(RSSConfig config) {
        List<URI> uris = config.getURIs();

        schemas = Map.of(
        "default",
            Map.of(
                "channel", new ChannelTable(uris),
                "item", new ItemTable(uris)
            )
        );
    }

    public Set<String> getSchemaNames() { return schemas.keySet(); }

    public Set<String> getTableNames(String schema) {
        requireNonNull(schema, "schema is null");
        Map<String, RSSTable> tables = schemas.get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public RSSTable getTable(String schema, String tableName) {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, RSSTable> tables = schemas.get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }
}