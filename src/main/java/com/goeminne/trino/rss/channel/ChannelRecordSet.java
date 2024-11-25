package com.goeminne.trino.rss.channel;

import com.goeminne.trino.rss.RSSColumnHandle;
import com.goeminne.trino.rss.RSSRecordSet;
import com.goeminne.trino.rss.RSSSplit;
import io.trino.spi.connector.RecordCursor;
import java.util.List;

public class ChannelRecordSet extends RSSRecordSet {

    public ChannelRecordSet(RSSSplit split, List<RSSColumnHandle> handles) {
        super(split, handles);
    }

    @Override
    public RecordCursor cursor() {
        return new ChannelRecordCursor(columnHandles, byteSource);
    }
}
