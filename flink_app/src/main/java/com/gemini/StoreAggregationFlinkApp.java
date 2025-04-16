package com.gemini;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

public class StoreAggregationFlinkApp
{
    public static void main(String[] args) throws Exception
    {
        final String kafkaSourceAddr = System.getenv("KAFKA_SOURCE_ADDR");
        final String consumerGroupId = System.getenv("KAFKA_CONS_GROUP_ID");
        final String inputTopic = System.getenv("INPUT_TOPIC");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        KafkaSource<OrderData> source = KafkaSource.<OrderData>builder()
                                            .setBootstrapServers(kafkaSourceAddr)
                                            .setTopics(inputTopic)
                                            .setGroupId(consumerGroupId)
                                            .setStartingOffsets(OffsetsInitializer.latest())
                                            .setValueOnlyDeserializer(new OrderDataDeserializationSchema())
                                            .build();

        FileSink<OrderData> sink = FileSink
                                       .forBulkFormat(
                                           new Path("output/"),
                                           AvroParquetWriters.forReflectRecord(OrderData.class))
                                       .withBucketAssigner(new OrderDataBucketAssigner())
                                       .build();

        env.enableCheckpointing(5000);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka input")
            .sinkTo(sink);

        env.execute("Flink Store Aggregation Job");
    }

    public static class OrderData
    {
        public double amount;
        public String storeId;
        public double lat;
        public double lng;
        public String orderStatus;

        public OrderData()
        {
        }

        public OrderData(double amount, String storeId, double lat, double lng, String orderStatus)
        {
            this.amount = amount;
            this.storeId = storeId;
            this.lat = lat;
            this.lng = lng;
            this.orderStatus = orderStatus;
        }
    }

    public static class OrderDataDeserializationSchema implements DeserializationSchema<OrderData>
    {
        public static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public OrderData deserialize(byte[] message)
        {
            try
            {
                JsonNode rootNode = mapper.readTree(message);
                JsonNode dataNode = mapper.readTree(rootNode.get("data").asText());
                JsonNode orderNode = dataNode.get("order");
                JsonNode storeNode = dataNode.get("store");
                return new OrderData(
                    orderNode.get("order_amount").asDouble(),
                    storeNode.get("store_id").asText(),
                    storeNode.at("/store_loc/store_lat").asDouble(),
                    storeNode.at("/store_loc/store_long").asDouble(),
                    orderNode.get("order_status").asText());
            }
            catch (IOException e)
            {
                e.printStackTrace();
                return new OrderData();
            }
        }

        @Override
        public boolean isEndOfStream(OrderData o)
        {
            return false;
        }

        @Override
        public TypeInformation<OrderData> getProducedType()
        {
            return TypeInformation.of(OrderData.class);
        }
    }

    public static class OrderDataBucketAssigner implements BucketAssigner<OrderData, String>
    {
        private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss").withZone(ZoneId.of("UTC"));

        @Override
        public String getBucketId(OrderData order, Context context)
        {
            long currentTimeMs = System.currentTimeMillis();
            long bucketStartMs = (currentTimeMs / 10000) * 10000;
            Instant bucketInstant = Instant.ofEpochMilli(bucketStartMs);
            String store = order.storeId;
            return String.format("storeId=%s/timestamp=%s", store, formatter.format(bucketInstant));
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer()
        {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }
}
