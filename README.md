Subscribing to Kafka Messages
=============================

Subscribing to a Kafka Topic and processing the messages received in realtime is a common requirement in
big data applications. In this guide, you will learn how to accomplish it with the Cask Data Application Platform
([CDAP](http://cdap.io)).

What You Will Build
-------------------

You will build a CDAP application that consumes data from a 0.8.x Kafka Cluster on a specific Topic and computes the 
average size of the messages received. You will:

- Build a a realtime 
  [Flow](http://docs.cdap.io/cdap/current/en/developer-guide/building-blocks/flows-flowlets/flows.html)
  to subscribes to a Kafka Topic;
- Build a Flowlet using the [cdap-pack-kafka](https://github.com/caskdata/cdap-packs) library
- Use a 
  [Dataset](http://docs.cdap.io/cdap/current/en/developer-guide/building-blocks/datasets/index.html)
  to persist the results of the analysis; and
- Build a 
  [Service](http://docs.cdap.io/cdap/current/en/developer-guide/building-blocks/services.html)
  to serve the analysis results via a RESTful endpoint.

What You Will Need
------------------

- [JDK 6 or JDK 7](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
- [Apache Maven 3.0+](http://maven.apache.org/)
- [CDAP SDK](http://docs.cdap.io/cdap/current/en/developer-guide/getting-started/standalone/index.html)

Let’s Build It!
---------------

Following sections will guide you through building an application from
scratch. If you are interested in deploying and running the application
right away, you can clone its source code from this GitHub repository.
In that case, feel free to skip the next two sections and jump right to
the [Configuring KafkaSubFlowlet](#configuring-kafkasubflowlet) section.

### Application Design

Realtime processing capability within CDAP is supported by a Flow. The
application we are building in this guide uses a Flow for processing the
messages received on a Kafka Topic. The count and size of these messages 
are persisted in a Dataset and are made available via RESTful endpoint 
using a Service.

![](docs/images/app-design.png)

The Flow consists of two processing nodes called Flowlets:

-   A subscriber Flowlet that subscribes to a specific topic on a Kafka Cluster 
    and emits the messages received to the next Flowlet.
-   A counter Flowlet that consumes the message emitted by the Kafka Subscriber 
    Flowlet to update the basic statistics of Kafka Messages: total message size and
    count.

### Application Implementation

The recommended way to build a CDAP application from scratch is to use a
Maven project. Use the following directory structure (you’ll find
contents of these files described below):

    ./pom.xml
    ./src/main/java/co/cask/cdap/guides/kafka/KafkaIngestionApp.java
    ./src/main/java/co/cask/cdap/guides/kafka/KafkaIngestionFlow.java
    ./src/main/java/co/cask/cdap/guides/kafka/KafkaMsgCounterFlowlet.java
    ./src/main/java/co/cask/cdap/guides/kafka/KafkaStatsHandler.java
    ./src/main/java/co/cask/cdap/guides/kafka/KafkaSubFlowlet.java

The application will use the `cdap-packs-kafka` library which includes an implementation of 
`Kafka08ConsumerFlowlet` which is designed to work with a 0.8.x Kakfa Cluster. If you want to 
use the application with 0.7.x Kakfa Cluster, please refer to `cdap-packs-kafka` for information 
on how to build a 0.7.x Kafka Subscriber Flowlet.

You'll need to add the correct `cdap-packs-kafka` library (based on your Kafka Cluster version, 
cdap-flow-compat-0.8 for this guide) as a dependency to your project's pom.xml:

```xml
...
<dependencies>
  ...
  <dependency>
    <groupId>co.cask.cdap.packs</groupId>
    <artifactId>cdap-kafka-flow-compat-0.8</artifactId>
    <version>0.1.0</version>
  </dependency>
</dependencies>
```

Create the `KafkaIngestionApp` class which declares that the application
has a Flow, a Service, and creates a Dataset:

```java
public class KafkaIngestionApp extends AbstractApplication {
  static final String NAME = "KafkaIngestion";
  static final String STATS_TABLE_NAME = "kafkaCounter";
  static final String SERVICE_NAME = "msgStatsService";
  static final String OFFSET_TABLE_NAME = "kafkaOffsets";

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Subscribe to Kafka Messages - Maintain overall count and size of messages received");
    createDataset(OFFSET_TABLE_NAME, KeyValueTable.class);
    createDataset(STATS_TABLE_NAME, KeyValueTable.class);
    addFlow(new KafkaIngestionFlow());
    addService(SERVICE_NAME, new KafkaStatsHandler());
  }
}
```

The `KafkaIngestionFlow` connects the `KafkaSubFlowlet` to `KafkaMsgCounterFlowlet`.

```java
public class KafkaIngestionFlow implements Flow {
  static final String NAME = "KafkaIngestionFlow";
  static final String KAFKA_FLOWLET = "kafkaSubFlowlet";
  static final String SINK_FLOWLET = "countMessages";

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName(NAME)
      .setDescription("Subscribes to Kafka Messages")
      .withFlowlets()
        .add(KAFKA_FLOWLET, new KafkaSubFlowlet())
        .add(SINK_FLOWLET, new KafkaMsgCounterFlowlet())
      .connect()
        .from(KAFKA_FLOWLET).to(SINK_FLOWLET)
      .build();
  }
}
```

The `KafkaSubFlowlet` makes use of the `Kafka08ConsumerFlowlet` that is
available in the `cdap-packs-kafka` library:

```java
public class KafkaSubFlowlet extends Kafka08ConsumerFlowlet<byte[], String> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSubFlowlet.class);
  @UseDataSet(KafkaIngestionApp.OFFSET_TABLE_NAME)
  private KeyValueTable offsetStore;
  private OutputEmitter<String> emitter;

  @Override
  protected void configureKafka(KafkaConfigurer kafkaConfigurer) {
    Map<String, String> runtimeArgs = getContext().getRuntimeArguments();
    if (runtimeArgs.containsKey("kafka.zookeeper")) {
      kafkaConfigurer.setZooKeeper(runtimeArgs.get("kafka.zookeeper"));
    } else if (runtimeArgs.containsKey("kafka.brokers")) {
      kafkaConfigurer.setBrokers(runtimeArgs.get("kafka.brokers"));
    }
    kafkaConfigurer.addTopicPartition(runtimeArgs.get("kafka.topic"), 0);
    kafkaConfigurer.addTopicPartition(runtimeArgs.get("kafka.topic"), 1);
  }

  @Override
  protected KeyValueTable getOffsetStore() {
    return offsetStore;
  }

  @Override
  protected void processMessage(String value) throws Exception {
    LOG.info("Message: {}", value);
    emitter.emit(value);
  }
}
```

Tweets pulled by the `TweetCollectorFlowlet` are consumed by the
`StatsRecorderFlowlet` that updates the total number of tweets and their
total body size in a Dataset:

```java
public class StatsRecorderFlowlet extends AbstractFlowlet {
  @UseDataSet(TwitterAnalysisApp.TABLE_NAME)
  private KeyValueTable statsTable;

  @ProcessInput
  public void process(Tweet tweet) {
    statsTable.increment(Bytes.toBytes("totalCount"), 1);
    statsTable.increment(Bytes.toBytes("totalSize"), tweet.getText().length());
  }
}
```

In a real-world scenario, the Flowlet could perform more sophisticated
processing on the messages received from Kafka.

Finally, the `KafkaStatsHandler` uses the `kafkaCounter` Dataset to compute the
average tweet size and serve it over HTTP:

```java
@Path("/v1")
public class KafkaStatsHandler extends AbstractHttpServiceHandler {
  @UseDataSet(KafkaIngestionApp.STATS_TABLE_NAME)
  private KeyValueTable statsTable;

  @Path("avgSize")
  @GET
  public void getStats(HttpServiceRequest request, HttpServiceResponder responder) throws Exception {
    long totalCount = statsTable.incrementAndGet(Bytes.toBytes(KafkaMsgCounterFlowlet.COUNTKEY), 0L);
    long totalSize = statsTable.incrementAndGet(Bytes.toBytes(KafkaMsgCounterFlowlet.SIZEKEY), 0L);
    responder.sendJson(totalCount > 0 ? totalSize / totalCount : 0);
  }
}

```

### Configuring `KafkaSubFlowlet`

In order to utilize the `KafkaSubFlowlet`, Kafka Zookeeper String *OR* Kafka Brokers information along with 
the Kafka Topic must be provided as runtime arguments. You can provide these to the `KafkaSubFlowlet` as 
runtime arguments of `KafkaIngestionFlow`. The keys of the required runtime arguments are:

```console
kafka.zookeeper
kafka.brokers
kafka.topic
```

Build & Run
-----------

The KafkaIngestionApp application can be built and packaged using the Apache Maven command:

    mvn clean package

Note that the remaining commands assume that the `cdap-cli.sh` script is
available on your PATH. If this is not the case, please add it:

    export PATH=$PATH:<CDAP home>/bin

If you haven't already started a standalone CDAP installation, start it with the command:

    cdap.sh start

We can then deploy the application to a standalone CDAP installation and
start its components:

    cdap-cli.sh deploy app target/cdap-kafka-ingest-guide-1.0.0.jar
    cdap-cli.sh start flow KafkaIngestion.KafkaIngestionFlow '--kafka.brokers=127.0.0.1:1234 --kafka.topic=MyTopic'
    cdap-cli.sh start service KafkaIngestion.KafkaStatsService

Once Flow is started, Kafka Messages are processed as they are published. You can query for
the average Kafka Message size:

    curl http://localhost:10000/v2/apps/KafkaIngestion/services/KafkaStatsService/methods/v1/avgSize

Example output:

    14

Share and Discuss!
------------------

Have a question? Discuss at the [CDAP User Mailing List.](https://groups.google.com/forum/#!forum/cdap-user)

License
-------

Copyright © 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain
a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

