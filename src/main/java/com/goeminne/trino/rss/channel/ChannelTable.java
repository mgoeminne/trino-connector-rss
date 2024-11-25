package com.goeminne.trino.rss.channel;

import com.goeminne.trino.rss.RSSColumn;
import com.goeminne.trino.rss.RSSTable;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarcharType;

import java.net.URI;
import java.util.List;

public class ChannelTable extends RSSTable {
    public ChannelTable(List<URI> sources) {
        super("channel", columns(), sources);
    }

    private static List<RSSColumn> columns() {
        return ImmutableList.of(
            new RSSColumn("title", VarcharType.VARCHAR),
            new RSSColumn("link", VarcharType.VARCHAR),
            new RSSColumn("description", VarcharType.VARCHAR),
            new RSSColumn("language", VarcharType.VARCHAR),
            new RSSColumn("webmaster", VarcharType.VARCHAR),
            new RSSColumn("pub_date", TimestampType.TIMESTAMP_SECONDS)
        );
    }
}
