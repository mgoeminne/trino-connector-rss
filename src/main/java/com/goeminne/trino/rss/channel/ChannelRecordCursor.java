package com.goeminne.trino.rss.channel;

import com.goeminne.trino.rss.RSSColumnHandle;
import com.goeminne.trino.rss.RSSRecordCursor;
import com.google.common.io.ByteSource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class ChannelRecordCursor extends RSSRecordCursor<ChannelRow> {

    public ChannelRecordCursor(List<RSSColumnHandle> columnHandles, ByteSource byteSource) {
        super(columnHandles, byteSource);
    }

    @Override
    public Iterator<ChannelRow> toIterator(ByteSource byteSource) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        try(InputStream is = byteSource.openBufferedStream()) {
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(is);
            document.getDocumentElement().normalize();

            Element channel = (Element) document.getElementsByTagName("channel").item(0);

            String title = getTagValue("title", channel);
            String link = getTagValue("link", channel);
            String description = getTagValue("description", channel);
            Optional<String> language = Optional.ofNullable(getTagValue("language", channel));
            Optional<String> webMaster = Optional.ofNullable(getTagValue("webMaster", channel));
            Optional<Instant> pubDate = Optional.ofNullable(getTagValue("pubDate", channel)).map(RSSRecordCursor::parseDate);

            ChannelRow row = new ChannelRow(
                title,
                link,
                description,
                language,
                webMaster,
                pubDate
            );

            return List.of(row).iterator();
        } catch (IOException | ParserConfigurationException | SAXException e) {
            throw new RuntimeException(e);
        }
    }
}
