package com.goeminne.trino.rss;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.Module;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class RSSModule implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(RSSConnector.class).in(Scopes.SINGLETON);
        binder.bind(RSSMetadata.class).in(Scopes.SINGLETON);
        binder.bind(RSSClient.class).in(Scopes.SINGLETON);
        binder.bind(RSSSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(RSSRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(RSSConfig.class);

        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(RSSTable.class));
    }
}