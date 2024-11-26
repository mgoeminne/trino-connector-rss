package com.goeminne.trino.rss;

import com.google.common.base.Strings;
import com.google.common.io.ByteSource;
import com.google.common.io.CountingInputStream;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.DateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public abstract class RSSRecordCursor<T extends RSSRow> implements RecordCursor
{
    private final List<RSSColumnHandle> columnHandles;

    private final Iterator<T> elements;
    private final long totalBytes;

    private List<String> fields;

    public RSSRecordCursor(List<RSSColumnHandle> columnHandles, ByteSource byteSource) {
        this.columnHandles = columnHandles;

        try (CountingInputStream input = new CountingInputStream(byteSource.openStream())) {
            elements = toIterator(byteSource);
            totalBytes = input.getCount();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Converts a byte source into an iterator of elements.
     * @param byteSource The underlying data source.
     * @return An iterator over the elements to be extracted from the source.
     */
    public abstract Iterator<T> toIterator(ByteSource byteSource);

    @Override
    public long getCompletedBytes() { return totalBytes; }

    @Override
    public long getReadTimeNanos() { return 0; }

    @Override
    public Type getType(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition() {
        if (!elements.hasNext()) {
            return false;
        }
        T element = elements.next();
        fields = element.toFields(columnHandles);

        return true;
    }

    private String getFieldValue(int field) {
        checkState(fields != null, "Cursor has not been advanced yet");
        return fields.get(field);
    }

    @Override
    public boolean getBoolean(int field) {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field) {
        Type fieldType = getType(field);

        if(List.of(
                TimestampType.TIMESTAMP_SECONDS,
                TimestampType.TIMESTAMP_MILLIS,
                TimestampType.TIMESTAMP_MICROS,
                TimestampType.TIMESTAMP_NANOS,
                TimestampType.TIMESTAMP_PICOS
        ).contains(fieldType)) {
            Instant i = Instant.parse(getFieldValue(field));
            return (i.getEpochSecond() * 1000000 + i.getNano() / 1000);
        }

        checkFieldType(field, BIGINT);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field) {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field) {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Object getObject(int field) {
        return getFieldValue(field);
    }

    @Override
    public boolean isNull(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected) {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    protected static String getTagValue(String tag, Element element) {
        NodeList nodeList = element.getElementsByTagName(tag);
        if (nodeList.getLength() > 0) {
            Node node = nodeList.item(0);
            if (node != null && node.getNodeType() == Node.ELEMENT_NODE) {
                return node.getTextContent();
            }
        }
        return null;
    }

    @Override
    public void close() {}

    protected static Instant parseDate(String date) {
        List<String> patterns = List.of(
                "yyyy-MM-dd'T'HH:mm:ss",
                "yyyy-MM-dd'T'HH:mm:ss Z",
                "yyyy-MM-dd HH:mm:ss",
                "yyyy-MM-dd HH:mm:ss Z",
                "yyyy-MM-dd HH:mm",
                "yyyy-MM-dd HH:mm Z",
                "yyyy-MM",
                "yyyy-MM Z",
                "EEEE, dd MMM yyyy HH:mm:ss",
                "EEEE, dd MMM yyyy HH:mm:ss z",
                "EEEE, dd MMM yyyy HH:mm:ss Z",
                "EEEE, dd MMM yyyy HH:mm",
                "EEEE, dd MMM yyyy HH:mm z",
                "EEEE, dd MMM yyyy HH:mm Z"
        );

        List<Locale> locales = List.of(DateFormat.getAvailableLocales());
        List<Optional<Instant>> results = new ArrayList<>(patterns.size() * locales.size());

        patterns.forEach(pattern -> {
            locales.forEach(locale -> {
                results.add(parseToInstant(date, pattern, locale));
            });
        });

        return results
           .stream()
           .filter(Optional::isPresent)
           .findFirst()
           .map(Optional::get)
           .orElseThrow(() -> new IllegalArgumentException("Invalid date: " + date));
    }

    private static Optional<Instant> parseToInstant(String dateString, String pattern, Locale locale) {
        try {
            // Define the formatter for the given pattern
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern, locale);

            // Attempt to parse as ZonedDateTime if the pattern includes timezone info
            if (pattern.contains("z") || pattern.contains("Z")) {
                ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateString, formatter);
                return Optional.of(zonedDateTime.toInstant());
            } else {
                // Parse as LocalDateTime and assume the system's default zone
                LocalDateTime localDateTime = LocalDateTime.parse(dateString, formatter);
                return Optional.of(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
            }
        } catch (DateTimeParseException e) {
            return Optional.empty();
        }
    }
}