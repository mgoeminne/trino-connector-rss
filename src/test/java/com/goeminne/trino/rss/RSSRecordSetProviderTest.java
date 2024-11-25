package com.goeminne.trino.rss;

import com.goeminne.trino.rss.channel.ChannelTable;
import com.goeminne.trino.rss.item.ItemTable;
import io.trino.spi.connector.*;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class RSSRecordSetProviderTest {

    @Test
    public void testChannelSingleURI() throws URISyntaxException {
        URI uri = new URI("https://feeds.bbci.co.uk/news/rss.xml");

        Connector connector = new RSSConnectorFactory().create(
            "rss",
            Map.of("rss.uris", uri.toString()),
            new TestingConnectorContext()
        );

        ChannelTable table = new ChannelTable(List.of(uri));

        RecordSet recordSet = connector
            .getRecordSetProvider()
            .getRecordSet(
                RSSTransactionHandle.INSTANCE,
                null,
                new RSSSplit(uri),
                new RSSTableHandle("default", table.getName()),
                IntStream.range(0, table.getColumns().size()).mapToObj(i -> {
                    List<RSSColumn> columns = table.getColumns();
                    return new RSSColumnHandle(columns.get(i).getName(), columns.get(i).getType(), i);
                }).toList()
            );

        try(RecordCursor cursor = recordSet.cursor()) {
            int count = 0;

            while(cursor.advanceNextPosition()) {
                count++;
            }

            Assertions.assertEquals(1, count);
        }
    }

    @Test
    public void testChannelMultipleURI() throws URISyntaxException {
        List<URI> uris = List.of(
            new URI("https://feeds.bbci.co.uk/news/rss.xml"),
            new URI("https://www.nasa.gov/rss/dyn/lg_image_of_the_day.rss")
        );

        Connector connector = new RSSConnectorFactory().create(
            "rss",
            Map.of("rss.uris", String.join(",", uris.stream().map(uri -> uri.toString()).toList())),
            new TestingConnectorContext()
        );

        ChannelTable table = new ChannelTable(uris);

        RecordSet recordSet = connector
            .getRecordSetProvider()
            .getRecordSet(
                    RSSTransactionHandle.INSTANCE,
                    null,
                    new RSSSplit(uris.getFirst()),
                    new RSSTableHandle("default", table.getName()),
                    IntStream.range(0, table.getColumns().size()).mapToObj(i -> {
                        List<RSSColumn> columns = table.getColumns();
                        return new RSSColumnHandle(columns.get(i).getName(), columns.get(i).getType(), i);
                    }).toList()
            );

        try(RecordCursor cursor = recordSet.cursor()) {
            int count = 0;

            while(cursor.advanceNextPosition()) {
                count++;
            }

            Assertions.assertEquals(1, count);
        }
    }

    @Test
    public void testItemSingleURI() throws URISyntaxException {
        URI uri = new URI("https://feeds.bbci.co.uk/news/rss.xml");

        Connector connector = new RSSConnectorFactory().create(
            "rss",
            Map.of("rss.uris", uri.toString()),
            new TestingConnectorContext()
        );

        ItemTable table = new ItemTable(List.of(uri));

        RecordSet recordSet = connector
            .getRecordSetProvider()
            .getRecordSet(
                RSSTransactionHandle.INSTANCE,
                null,
                new RSSSplit(uri),
                new RSSTableHandle("default", table.getName()),
                IntStream.range(0, table.getColumns().size()).mapToObj(i -> {
                    List<RSSColumn> columns = table.getColumns();
                    return new RSSColumnHandle(columns.get(i).getName(), columns.get(i).getType(), i);
                }).toList()
            );

        try(RecordCursor cursor = recordSet.cursor()) {
            int count = 0;

            while(cursor.advanceNextPosition()) {
                count++;
            }

            Assertions.assertTrue(count >= 1);
        }
    }
}
