package com.sunil;

import com.sunil.coders.RecordSerializableCoder;
import com.sunil.loggers.WriteOneFilePerWindow;
import com.sunil.objects.Record;
import com.sunil.policies.CustomFieldTimePolicy;
import com.sunil.serde.RecordDeserializer;
import com.sunil.transforms.CountWords;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class WindowedWordCount {

    static void runWithOptions(WindowedWordCountOptions options) {
        Pipeline pipeline = Pipeline.create(options);
        Duration WINDOW_TIME = Duration.standardMinutes(1);
        Duration ALLOWED_LATENESS = Duration.standardMinutes(1);

        CoderRegistry cr = pipeline.getCoderRegistry();
        cr.registerCoderForClass(Record.class, new RecordSerializableCoder());


        pipeline.apply(
                KafkaIO.<Long, Record>read()
                        .withBootstrapServers(options.getBootstrap())
                        .withTopic(options.getInputTopic())
                        .withKeyDeserializer(LongDeserializer.class)
                        .withValueDeserializer(RecordDeserializer.class)
                        .withTimestampPolicyFactory((tp, previousWaterMark) -> new CustomFieldTimePolicy(previousWaterMark))
                        .withoutMetadata()
        )
                .apply(Values.<Record>create())
                .apply("append event time for PCollection records", WithTimestamps.of((Record rec) -> new Instant(rec.getEts())))
                .apply("extract message string", MapElements
                        .into(TypeDescriptors.strings())
                        .via(Record::getMessage))
                .apply("apply window", Window.<String>into(FixedWindows.of(WINDOW_TIME))
                        .withAllowedLateness(ALLOWED_LATENESS)
                        .triggering(AfterWatermark.pastEndOfWindow())
                        .accumulatingFiredPanes()
                )
                .apply("count words", new CountWords())
                .apply("format result to String",MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Long> rec) -> rec.getKey() + ":" + rec.getValue()))
                .apply("Write it to a text file", new WriteOneFilePerWindow(options.getOutput()));


        pipeline.run();
    }

    public static void main(String[] args) {
        WindowedWordCountOptions options = PipelineOptionsFactory.fromArgs(args).as(WindowedWordCountOptions.class);
        options.setStreaming(true);
        runWithOptions(options);
    }
}
