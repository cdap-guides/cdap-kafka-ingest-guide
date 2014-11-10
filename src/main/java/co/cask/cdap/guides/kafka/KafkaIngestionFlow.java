package co.cask.cdap.guides.kafka;

import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;

/**
 * Flow to ingest Kafka Messages (works with Kafka 0.8.x cluster).
 * <p>
 * Requires these runtime arguments:
 * <ul>
 * <li>kafka.zookeeper: Kafka Zookeeper connection string</li>
 * <li>kafka.topic: Subscribe to Kafka Topic</li>
 * </ul>
 * </p>
 */
public class KafkaIngestionFlow implements Flow {

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName(Constants.FLOW_NAME)
      .setDescription("Subscribes to Kafka Messages")
      .withFlowlets()
        .add(Constants.KAFKA_FLOWLET, new KafkaSubFlowlet())
        .add(Constants.COUNTER_FLOWLET, new KafkaMsgCounterFlowlet())
      .connect()
        .from(Constants.KAFKA_FLOWLET).to(Constants.COUNTER_FLOWLET)
      .build();
  }
}
