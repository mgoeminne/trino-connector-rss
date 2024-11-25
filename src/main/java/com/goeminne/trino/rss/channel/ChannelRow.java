package com.goeminne.trino.rss.channel;

import com.goeminne.trino.rss.RSSColumnHandle;
import com.goeminne.trino.rss.RSSRow;

import java.time.Instant;
import java.util.List;
import java.util.Optional;


public class ChannelRow implements RSSRow {

    private final String title;
    private final String link;
    private final String description;
    private final Optional<String> language;
    private final Optional<String> webMaster;
    private final Optional<Instant> pubDate;

    /**
     *
     * @param title         The title of the channel. Should contain its name.
     * @param link          URL of the website that provides this channel.
     * @param description   A summary of the content proposed by the channel.
     * @param language      The human language used by this channel.
     * @param webMaster     The mail address of the responsible for this channel.
     * @param pubDate       The publishing date of the chanel
     */
    public ChannelRow (
        String title,
        String link,
        String description,
        Optional<String> language,
        Optional<String> webMaster,
        Optional<Instant> pubDate
    ) {
        this.title = title;
        this.link = link;
        this.description = description;
        this.language = language;
        this.webMaster = webMaster;
        this.pubDate = pubDate;
    }

    @Override
    public List<String> toFields(List<RSSColumnHandle> columnHandles) {
        return columnHandles.stream().map(columnHandle -> switch(columnHandle.getColumnName()) {
            case "title" -> title;
            case "link" -> link;
            case "description" -> description;
            case "language" -> language.orElse(null);
            case "webmaster" -> webMaster.orElse(null);
            case "pub_date" -> pubDate.map(x -> x.toString()).orElse(null);
            default -> throw new IllegalStateException("Unexpected value: " + columnHandle.getColumnName());
        }).toList();
    }
}
