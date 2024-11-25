package com.goeminne.trino.rss;

import com.google.common.io.ByteSource;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.net.MalformedURLException;
import java.util.List;

import static java.util.Objects.requireNonNull;

public abstract class RSSRecordSet implements RecordSet
{
    protected final List<RSSColumnHandle> columnHandles;
    protected final List<Type> columnTypes;
    protected final ByteSource byteSource;

    public RSSRecordSet(RSSSplit split, List<RSSColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.columnTypes = columnHandles.stream().map(RSSColumnHandle::getColumnType).toList();

        try {
            byteSource = new ResourceManager().asByteSource(split.getUri());
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public final List<Type> getColumnTypes() {
        return columnTypes;
    }
}
