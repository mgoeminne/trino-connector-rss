# Introduction

This is a Trino plugin that offers a connector for RSS feeds.

By using this plugin, you can configure catalogs that use RSS feeds as data source.

# How to use it?

The simplest way to use this plugin consists in compiling it and using it in your Trino cluster.

1. Compile the plugin by using Maven.

```shell
mvn clean package
```

This will create a file named `trino-connector-rss-X.Y.jar` or `trino-connector-rss-X.Y-SNAPSHOT.jar` in the `target` directory of the plugin project. For fat jars, a file named `trino-connector-rss.X.Y-jar-with-dependencies.jar` or `trino-connector-rss.X.Y-SNAPSHOT-jar-with-dependencies.jar` is created.

2. Create a configuration file `rss.properties` using the connector offered by the plugin.

```.properties
connector.name=rss
rss.uris=https://feeds.bbci.co.uk/news/rss.xml,https://www.nasa.gov/rss/dyn/lg_image_of_the_day.rss
```

Please note multiple RSS feeds can be used simultaneously. You simply have to declare them on the configuration file, separated with a comma.

3. Run an instance of a Trino image, and mount both the configuration file and the generated plugin jar. This requires a running Docker.

```shell
docker run -p 8080:8080 --mount type=bind,source="/home/myself/rss.properties",target="/etc/trino/catalog/rss.properties" --mount type=bind,source="/home/myself/project/target/trino-connector-rss-X.Y.jar",target="/etc/trino/plugin/rss.jar" --name trino trinodb/trino:464
```

# License

MIT License

Copyright (c) 2024 Mathieu Goeminne

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.