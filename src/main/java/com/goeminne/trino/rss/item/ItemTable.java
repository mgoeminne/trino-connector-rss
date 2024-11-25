package com.goeminne.trino.rss.item;

import com.goeminne.trino.rss.RSSColumn;
import com.goeminne.trino.rss.RSSTable;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarcharType;

import java.net.URI;
import java.util.List;

public class ItemTable extends RSSTable {
    public ItemTable(List<URI> sources) {
        super("item", columns(), sources);
    }

    private static List<RSSColumn> columns() {
        return ImmutableList.of(
            new RSSColumn("channel_link", VarcharType.VARCHAR),
            new RSSColumn("title", VarcharType.VARCHAR),
            new RSSColumn("link", VarcharType.VARCHAR),
            new RSSColumn("description", VarcharType.VARCHAR),
            new RSSColumn("pub_date", TimestampType.TIMESTAMP_SECONDS),
            new RSSColumn("guid", VarcharType.VARCHAR),
            new RSSColumn("category", VarcharType.VARCHAR)
        );
    }
}
