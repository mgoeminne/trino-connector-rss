package com.goeminne.trino.rss;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class RSSTableHandle implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;

    @JsonCreator
    public RSSTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName) {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    @JsonProperty
    public String getSchemaName() { return schemaName; }

    @JsonProperty
    public String getTableName() { return tableName; }

    public SchemaTableName toSchemaTableName() { return new SchemaTableName(schemaName, tableName); }

    @Override
    public int hashCode() { return Objects.hash(schemaName, tableName); }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        RSSTableHandle other = (RSSTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString() { return schemaName + ":" + tableName; }
}