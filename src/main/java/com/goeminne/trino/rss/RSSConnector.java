package com.goeminne.trino.rss;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import java.net.URI;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class RSSConnector implements Connector {

    private final LifeCycleManager lifeCycleManager;
    private RSSMetadata metadata;
    private final RSSSplitManager splitManager;
    private final RSSRecordSetProvider recordSetProvider;

    @Inject
    public RSSConnector(
            LifeCycleManager lifeCycleManager,
            RSSSplitManager splitManager,
            RSSRecordSetProvider recordSetProvider) {

        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");

        RSSClient client = new RSSClient(new RSSConfig());
        this.metadata = new RSSMetadata(client);
    }

    public RSSConnector withUris(List<URI> uris) {
        RSSConfig config = new RSSConfig();
        config.setURIs(uris);
        RSSClient client = new RSSClient(config);
        this.metadata = new RSSMetadata(client);
        return this;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return RSSTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() { return splitManager; }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() { return recordSetProvider; }

    @Override
    public final void shutdown() {
        lifeCycleManager.stop();
    }
}