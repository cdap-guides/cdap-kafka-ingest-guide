package co.cask.cdap.guides.kafka;

import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;

/**
 * Flow to ingest Kafka Messages (Works with Kafka 0.8.x cluster).
 * KafkaIngestionFlow expects the following runtime arguments.
 * kafka.zookeeper (or) kafka.brokers - Kafka Instance to connect.
 * kafka.topic - Kafka Topic to subscribe.
 */
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
