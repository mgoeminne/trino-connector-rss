package com.goeminne.trino.rss.item;

import com.goeminne.trino.rss.RSSColumnHandle;
import com.goeminne.trino.rss.RSSRow;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 *
 * @param channelLink  The channel this item belongs to.
 * @param title         The title of the item.
 * @param link          The link of the item.
 * @param description   A displayable description of the item.
 * @param pubDate       The date at which the item has been published.
 * @param guid          A unique identifier for the item.
 * @param category      The category this item belongs to.
 */
public record ItemRow(
    String channelLink, String title, String link, String description,
    Optional<Instant> pubDate, Optional<String> guid, Optional<String> category
) implements RSSRow {

    @Override
    public List<String> toFields(List<RSSColumnHandle> columnHandles) {
        return columnHandles.stream().map(columnHandle -> switch(columnHandle.getColumnName()) {
            case "channel_link" -> channelLink;
            case "title" -> title;
            case "link" -> link;
            case "description" -> description;
            case "pub_date" -> pubDate.map(Instant::toString).orElse(null);
            case "guid" -> guid.orElse(null);
            case "category" -> category.orElse(null);
            default -> throw new IllegalStateException("Unexpected value: " + columnHandle.getColumnName());
        }).toList();
    }
}
