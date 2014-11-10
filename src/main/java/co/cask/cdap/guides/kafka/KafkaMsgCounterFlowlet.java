package co.cask.cdap.guides.kafka;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flowlet to store the statistics of the Messages received.
 */
public class KafkaMsgCounterFlowlet extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMsgCounterFlowlet.class);
  static final String COUNTKEY = "totalCount";
  static final String SIZEKEY = "totalSize";

  @UseDataSet(KafkaIngestionApp.STATS_TABLE_NAME)
  private KeyValueTable counter;

  @ProcessInput
  public void process(String string) {
    LOG.info("Received: {}", string);
    counter.increment(Bytes.toBytes(COUNTKEY), 1L);
    counter.increment(Bytes.toBytes(SIZEKEY), string.length());
  }
}
