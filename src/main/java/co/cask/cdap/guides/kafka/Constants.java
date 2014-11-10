package co.cask.cdap.guides.kafka;

/**
 *
 */
public final class Constants {
  public static final String FLOW_NAME = "KafkaIngestionFlow";
  public static final String SERVICE_NAME = "KafkaStatsService";
  public static final String STATS_TABLE_NAME = "kafkaCounter";
  public static final String OFFSET_TABLE_NAME = "kafkaOffsets";
  public static final String KAFKA_FLOWLET = "kafkaSubFlowlet";
  public static final String COUNTER_FLOWLET = "countMessages";
  public static final String COUNT_KEY = "totalCount";
  public static final String SIZE_KEY = "totalSize";

  private Constants () {
  }
}
