package com.goeminne.trino.rss;

import com.google.common.io.ByteSource;
import com.google.common.io.Resources;

import java.net.MalformedURLException;
import java.net.URI;

public class ResourceManager {

    public ByteSource asByteSource(URI uri) throws MalformedURLException {
        return Resources.asByteSource(uri.toURL());
    }
}
