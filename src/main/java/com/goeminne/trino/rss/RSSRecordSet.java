package com.goeminne.trino.rss;

import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class RSSRecordSet implements RecordSet
{
    private final List<RSSColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final ByteSource byteSource;

    public RSSRecordSet(RSSSplit split, List<RSSColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.columnTypes = columnHandles.stream().map(RSSColumnHandle::getColumnType).toList();

        try {
            byteSource = Resources.asByteSource(URI.create(split.getUri()).toURL());
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new RSSRecordCursor(columnHandles, byteSource);
    }
}
