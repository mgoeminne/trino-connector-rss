package com.goeminne.trino.rss.item;

import com.goeminne.trino.rss.RSSColumnHandle;
import com.goeminne.trino.rss.RSSRecordCursor;
import com.google.common.io.ByteSource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class ItemRecordCursor extends RSSRecordCursor<ItemRow> {

    public ItemRecordCursor(List<RSSColumnHandle> columnHandles, ByteSource byteSource) {
        super(columnHandles, byteSource);
    }

    @Override
    public Iterator<ItemRow> toIterator(ByteSource byteSource) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        try(InputStream is = byteSource.openStream()) {
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(is);
            document.getDocumentElement().normalize();

            Element channel = (Element) document.getElementsByTagName("channel").item(0);
            String channelLink = getTagValue("link", channel);

            NodeList items = document.getElementsByTagName("item");
            List<ItemRow> ret = new ArrayList<>(items.getLength());

            for(int i=0 ; i<items.getLength() ; i++) {
                Element item = (Element) items.item(i);

                String title = getTagValue("title", item);
                String link = getTagValue("link", item);
                String description = getTagValue("description", item);
                Optional<Instant> pubDate = Optional.ofNullable(getTagValue("pubDate", item)).map(RSSRecordCursor::parseDate);
                Optional<String> guid = Optional.ofNullable(getTagValue("guid", item));
                Optional<String> category = Optional.ofNullable(getTagValue("category", item));

                ret.add(new ItemRow(
                    channelLink,
                    title,
                    link,
                    description,
                    pubDate,
                    guid,
                    category
                ));
            }

            return ret.iterator();
        } catch (IOException | ParserConfigurationException | SAXException e) {
            throw new RuntimeException(e);
        }
    }
}
