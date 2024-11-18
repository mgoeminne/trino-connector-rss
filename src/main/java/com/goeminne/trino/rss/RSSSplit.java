package com.goeminne.trino.rss;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class RSSSplit implements ConnectorSplit {
    private static final int INSTANCE_SIZE = instanceSize(RSSSplit.class);

    private final String uri;
    private final boolean remotelyAccessible;
    private final List<HostAddress> addresses;

    @JsonCreator
    public RSSSplit(@JsonProperty("uri") String uri)
    {
        this.uri = requireNonNull(uri, "uri is null");

        remotelyAccessible = true;
        addresses = ImmutableList.of(HostAddress.fromUri(URI.create(uri)));
    }

    @JsonProperty
    public String getUri() {
        return uri;
    }

    @Override
    public boolean isRemotelyAccessible() {
        // only http or https is remotely accessible
        return remotelyAccessible;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return addresses;
    }

    @Override
    public Map<String, String> getSplitInfo() {
        return ImmutableMap.of(
                "addresses",
                addresses.stream().map(HostAddress::toString).collect(joining(",")),
                "remotelyAccessible",
                String.valueOf(remotelyAccessible)
        );
    }

    @Override
    public long getRetainedSizeInBytes() {
        return INSTANCE_SIZE
                + estimatedSizeOf(uri)
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes);
    }
}