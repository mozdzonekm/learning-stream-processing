package org.mozdzonekmit.learning.flink;

import org.apache.commons.cli.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.mozdzonekmit.learning.flink.io.JsonDeserializationSchema;
import org.mozdzonekmit.learning.flink.io.JsonSerializationSchema;
import org.mozdzonekmit.learning.flink.model.DeserializationResult;
import org.mozdzonekmit.learning.flink.model.Event;
import org.mozdzonekmit.learning.flink.model.Pair;
import org.mozdzonekmit.learning.flink.processing.DeserializationResultSplitter;
import org.mozdzonekmit.learning.flink.processing.PairProcessor;

public class DataStreamJob {

  public static void main(String[] args) throws Exception {
    // Arguments
    CommandLine cmd = parseCommandLine(args);
    String bootstrapServers = cmd.getOptionValue("bootstrapServers");
    String rawTopic = cmd.getOptionValue("rawTopic");
    String groupId = cmd.getOptionValue("groupId");
    String enrichedTopic = cmd.getOptionValue("enrichedTopic");
    String dlqTopic = cmd.getOptionValue("DLQ");
    long pairSum = Long.parseLong(cmd.getOptionValue("pairSum"));

    // Components
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    final OutputTag<byte[]> dlqTag = new OutputTag<>("dlq") {};

    KafkaSource<DeserializationResult<Event>> source =
        createKafkaSource(env, bootstrapServers, rawTopic, groupId);

    KafkaSink<byte[]> dlqSink = buildDlqSink(bootstrapServers, dlqTopic);
    KafkaSink<Pair> pairSink = buildPairsSink(bootstrapServers, enrichedTopic);

    // Processing
    SingleOutputStreamOperator<Event> events =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "read-events")
            .process(new DeserializationResultSplitter<>(dlqTag))
            .returns(Event.class);

    events.getSideOutput(dlqTag).sinkTo(dlqSink).name("dlq");

    events
        .filter(event -> event.getNum() >= 0 && event.getNum() <= pairSum)
        .keyBy(event -> event.getPairKey(pairSum))
        .process(new PairProcessor(pairSum))
        .name("pair-processor")
        .sinkTo(pairSink)
        .name("send-pairs");

    env.execute("Flink Event Pairer");
  }

  private static CommandLine parseCommandLine(String[] args) throws ParseException {
    Options options = new Options();
    options.addOption("s", "pairSum", true, "Pair sum value");
    options.addOption("b", "bootstrapServers", true, "Kafka bootstrap servers");
    options.addOption("r", "rawTopic", true, "Raw Kafka topic name");
    options.addOption("g", "groupId", true, "Kafka consumer group ID");
    options.addOption("e", "enrichedTopic", true, "Enriched Kafka topic name");
    options.addOption("d", "DLQ", true, "Dead Letter Queue for incorrect raw events");
    CommandLineParser parser = new DefaultParser();
    return parser.parse(options, args);
  }

  private static KafkaSource<DeserializationResult<Event>> createKafkaSource(
      StreamExecutionEnvironment env, String bootstrapServers, String rawTopic, String groupId) {
    return KafkaSource.<DeserializationResult<Event>>builder()
        .setBootstrapServers(bootstrapServers)
        .setTopics(rawTopic)
        .setGroupId(groupId)
        .setValueOnlyDeserializer(new JsonDeserializationSchema<>(Event.class))
        .build();
  }

  private static KafkaSink<byte[]> buildDlqSink(String bootstrapServers, String dlqTopic) {
    return KafkaSink.<byte[]>builder()
        .setBootstrapServers(bootstrapServers)
        .setTransactionalIdPrefix("dlq-sink-transactions")
        .setRecordSerializer(
            KafkaRecordSerializationSchema.<byte[]>builder()
                .setTopic(dlqTopic)
                .setValueSerializationSchema(
                    new SerializationSchema<byte[]>() {
                      @Override
                      public byte[] serialize(byte[] value) {
                        return value;
                      }
                    })
                .build())
        .build();
  }

  private static KafkaSink<Pair> buildPairsSink(String bootstrapServers, String enrichedTopic) {
    return KafkaSink.<Pair>builder()
        .setBootstrapServers(bootstrapServers)
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder()
                .setTopic(enrichedTopic)
                .setValueSerializationSchema(new JsonSerializationSchema<Pair>())
                .build())
        .build();
  }
}
