package com.goeminne.trino.rss;

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.TableNotFoundException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public class RSSSplitManager implements ConnectorSplitManager {
    private final RSSClient rssClient;

    public RSSSplitManager(RSSClient rssClient)
    {
        this.rssClient = requireNonNull(rssClient, "rssClient is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint) {
        RSSTableHandle tableHandle = (RSSTableHandle) connectorTableHandle;
        RSSTable table = rssClient.getTable(tableHandle.schemaName(), tableHandle.tableName());

        // this can happen if table is removed during a query
        if (table == null) {
            throw new TableNotFoundException(tableHandle.toSchemaTableName());
        }

        List<RSSSplit> splits = table.getSources().stream().map(uri -> new RSSSplit(uri)).collect(toCollection(ArrayList::new));
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }
}