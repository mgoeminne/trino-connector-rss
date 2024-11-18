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

import static java.util.Objects.requireNonNull;

public class RSSConnector implements Connector {

    private final LifeCycleManager lifeCycleManager;
    private final RSSMetadata metadata;
    private final RSSSplitManager splitManager;
    private final RSSRecordSetProvider recordSetProvider;

    @Inject
    public RSSConnector(
            LifeCycleManager lifeCycleManager,
            RSSMetadata metadata,
            RSSSplitManager splitManager,
            RSSRecordSetProvider recordSetProvider) {

        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
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