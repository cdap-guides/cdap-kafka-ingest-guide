package co.cask.cdap.guides.kafka;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Service that exposes an HTTP Endpoint to access the statistics on Kafka Messages stored in
 * a {@link KeyValueTable} dataset.
 */
@Path("/v1")
public class KafkaStatsHandler extends AbstractHttpServiceHandler {

  @UseDataSet(Constants.STATS_TABLE_NAME)
  private KeyValueTable statsTable;

  @Path("avgSize")
  @GET
  public void getStats(HttpServiceRequest request, HttpServiceResponder responder) throws Exception {
    long totalCount = statsTable.incrementAndGet(Bytes.toBytes(Constants.COUNT_KEY), 0L);
    long totalSize = statsTable.incrementAndGet(Bytes.toBytes(Constants.SIZE_KEY), 0L);
    int avgSize = (totalCount > 0) ? (int) (totalSize / totalCount) : 0;
    responder.sendJson(avgSize);
  }
}
