package com.sunil.policies;

import com.sunil.objects.Record;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

import java.util.Optional;

/**
 * Custom TimestampPolicy for Kafka source to manage timestamp and watermark when it pulls data from broker
 */
public class CustomFieldTimePolicy extends TimestampPolicy<Long, Record> {


    protected Instant currentWatermark;

    public CustomFieldTimePolicy(Optional<Instant> previousWatermark) {
        currentWatermark = previousWatermark.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }


    @Override
    public Instant getTimestampForRecord(PartitionContext ctx, KafkaRecord<Long, Record> record) {
        currentWatermark = new Instant(record.getKV().getValue().getTimestamp());
        return currentWatermark;
    }

    @Override
    public Instant getWatermark(PartitionContext ctx) {
        return currentWatermark;
    }
}