package co.cask.cdap.guides.kafka;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet;
import co.cask.cdap.kafka.flow.KafkaConfigurer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Kafka Subscription Flowlet.
 */
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
