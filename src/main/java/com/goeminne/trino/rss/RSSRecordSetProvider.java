package com.goeminne.trino.rss;

import com.goeminne.trino.rss.channel.ChannelRecordSet;
import com.goeminne.trino.rss.item.ItemRecordSet;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import java.util.List;

public class RSSRecordSetProvider implements ConnectorRecordSetProvider
{
    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        RSSSplit rssSplit = (RSSSplit) split;
        RSSTableHandle tableHandle = (RSSTableHandle) table;

        ImmutableList.Builder<RSSColumnHandle> handleBuilder = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handleBuilder.add((RSSColumnHandle) handle);
        }

        List<RSSColumnHandle> handles = handleBuilder.build();

        return switch(tableHandle.schemaName()) {
            case "default" -> switch(tableHandle.tableName()) {
                case "channel" -> new ChannelRecordSet(rssSplit, handles);
                case "item" -> new ItemRecordSet(rssSplit, handles);
                default -> throw new IllegalStateException("Unexpected table value for " + tableHandle.schemaName() + " schema: "  + tableHandle.schemaName());
            };
            default -> throw new IllegalStateException("Unexpected schema value: " + tableHandle.schemaName());
        };
    }
}