package com.goeminne.trino.rss;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.stream.Collectors.joining;

public class RSSSplit implements ConnectorSplit {
    private static final int INSTANCE_SIZE = instanceSize(RSSSplit.class);

    private final URI uri;
    private final List<HostAddress> addresses;

    @JsonCreator
    public RSSSplit(@JsonProperty("uri") URI uri)
    {
        this.uri = uri;
        addresses = List.of(HostAddress.fromUri(uri));
    }

    @JsonProperty
    public URI getUri() { return uri; }

        @Override
    public List<HostAddress> getAddresses() { return addresses; }

    @Override
    public Map<String, String> getSplitInfo() {
        return Map.of(
                "addresses",
                addresses.stream().map(HostAddress::toString).collect(joining(",")),
                "remotelyAccessible",
                String.valueOf(isRemotelyAccessible())
        );
    }

    @Override
    public long getRetainedSizeInBytes() {
        return INSTANCE_SIZE
                + estimatedSizeOf(uri.toString())
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes);
    }

    @Override
    public String toString() {
        return uri.toString();
    }
}