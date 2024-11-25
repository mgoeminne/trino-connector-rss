package com.goeminne.trino.rss;

import java.util.List;

public interface RSSRow {
    List<String> toFields(List<RSSColumnHandle> columnHandles);
}
