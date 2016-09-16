/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.guides.kafka;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Test for {@link KafkaIngestionApp}.
 */
public class KafkaIngestionAppTest extends TestBase {
  private static final String KAFKA_TOPIC = "someTopic";
  private static ZKClientService zkClient;
  private static KafkaClientService kafkaClient;

  private static InMemoryZKServer zkServer;
  private static EmbeddedKafkaServer kafkaServer;
  private static int kafkaPort;

  @BeforeClass
  public static void init() throws Exception {
    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    kafkaPort = Networks.getRandomPort();
    kafkaServer = new EmbeddedKafkaServer(generateKafkaConfig(zkServer.getConnectionStr(), kafkaPort,
                                                              TMP_FOLDER.newFolder()));
    kafkaServer.startAndWait();

    zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    kafkaClient = new ZKKafkaClientService(zkClient);
    kafkaClient.startAndWait();
  }

  /**
   * A {@link com.google.common.util.concurrent.Service} implementation for running an instance of Kafka server in
   * the same process.
   */
  public static final class EmbeddedKafkaServer extends AbstractIdleService {

    private final KafkaServerStartable server;

    public EmbeddedKafkaServer(Properties properties) {
      server = new KafkaServerStartable(new KafkaConfig(properties));
    }

    @Override
    protected void startUp() throws Exception {
      server.startup();
    }

    @Override
    protected void shutDown() throws Exception {
      server.shutdown();
      server.awaitShutdown();
    }
  }

  @AfterClass
  public static void cleanup() {
    kafkaClient.stopAndWait();
    zkClient.stopAndWait();
    kafkaServer.stopAndWait();
    zkServer.stopAndWait();
  }

  @Test
  public void test() throws TimeoutException, InterruptedException, IOException {
    Map<String, String> runtimeArgs = Maps.newHashMap();
    runtimeArgs.put("kafka.topic", KAFKA_TOPIC);
    runtimeArgs.put("kafka.zookeeper", zkServer.getConnectionStr());

    // Deploy the KafkaIngestionApp application
    ApplicationManager appManager = deployApplication(KafkaIngestionApp.class);
    FlowManager flowManager = appManager.getFlowManager(Constants.FLOW_NAME).start(runtimeArgs);
    ServiceManager serviceManager = appManager.getServiceManager(Constants.SERVICE_NAME).start();
    serviceManager.waitForStatus(true);

    for (int i = 0; i < 10; i++) {
      sendMessage(KAFKA_TOPIC, ImmutableMap.of(Integer.toString(i), "message" + i));
    }

    RuntimeMetrics countMetrics = flowManager.getFlowletMetrics(Constants.COUNTER_FLOWLET);
    countMetrics.waitForProcessed(10, 10, TimeUnit.SECONDS);
    try {
      URL serviceURL = serviceManager.getServiceURL();
      URL url = new URL(serviceURL, "v1/avgSize");
      HttpRequest request = HttpRequest.get(url).build();
      HttpResponse response = HttpRequests.execute(request);
      Assert.assertEquals(200, response.getResponseCode());
      Assert.assertEquals("8", response.getResponseBodyAsString());
    } finally {
      serviceManager.stop();
      serviceManager.waitForStatus(false);
    }
    appManager.stopAll();
  }

  private void sendMessage(String topic, Map<String, String> messages) {
    Properties prop = new Properties();
    prop.setProperty("zk.connect", zkServer.getConnectionStr());
    prop.setProperty("serializer.class", "kafka.serializer.StringEncoder");

    ProducerConfig prodConfig = new ProducerConfig(prop);

    List<ProducerData<String, String>> outMessages = new ArrayList<>();
    for (Map.Entry<String, String> entry : messages.entrySet()) {
      outMessages.add(new ProducerData<>(topic, entry.getKey(), ImmutableList.of(entry.getValue())));
    }
    Producer<String, String> producer = new Producer<>(prodConfig);
    producer.send(outMessages);
  }

  private static Properties generateKafkaConfig(String zkConnectStr, int port, File logDir) {
    Properties prop = new Properties();
    prop.setProperty("log.dir", logDir.getAbsolutePath());
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("broker.id", "1");
    prop.setProperty("socket.send.buffer.bytes", "1048576");
    prop.setProperty("socket.receive.buffer.bytes", "1048576");
    prop.setProperty("socket.request.max.bytes", "104857600");
    prop.setProperty("num.partitions", "1");
    prop.setProperty("log.retention.hours", "24");
    prop.setProperty("log.flush.interval.messages", "10000");
    prop.setProperty("log.flush.interval.ms", "1000");
    prop.setProperty("log.segment.bytes", "536870912");
    prop.setProperty("zookeeper.connect", zkConnectStr);
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");
    prop.setProperty("default.replication.factor", "1");

    // These are for Kafka-0.7
    prop.setProperty("brokerid", "1");
    prop.setProperty("zk.connect", zkConnectStr);
    prop.setProperty("zk.connectiontimeout.ms", "1000000");
    prop.setProperty("log.retention.size", "1000");
    prop.setProperty("log.cleanup.interval.mins", "1");
    prop.setProperty("log.file.size", "1000");

    return prop;
  }
}
