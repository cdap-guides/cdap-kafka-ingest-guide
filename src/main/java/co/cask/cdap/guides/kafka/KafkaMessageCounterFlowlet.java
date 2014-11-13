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
public class KafkaMessageCounterFlowlet extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageCounterFlowlet.class);

  @UseDataSet(Constants.STATS_TABLE_NAME)
  private KeyValueTable counter;

  @ProcessInput
  public void process(String string) {
    LOG.info("Received: {}", string);
    counter.increment(Bytes.toBytes(Constants.COUNT_KEY), 1L);
    counter.increment(Bytes.toBytes(Constants.SIZE_KEY), string.length());
  }
}
