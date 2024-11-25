package com.goeminne.trino.rss.item;

import com.goeminne.trino.rss.RSSColumnHandle;
import com.goeminne.trino.rss.RSSRecordSet;
import com.goeminne.trino.rss.RSSSplit;
import io.trino.spi.connector.RecordCursor;

import java.util.List;

public class ItemRecordSet extends RSSRecordSet {

    public ItemRecordSet(RSSSplit split, List<RSSColumnHandle> handles) {
        super(split, handles);
    }

    @Override
    public RecordCursor cursor() {
        return new ItemRecordCursor(columnHandles, byteSource);
    }
}
