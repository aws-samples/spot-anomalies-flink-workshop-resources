/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.amazonaws.proserve.workshop;

import com.amazonaws.proserve.workshop.process.model.Event;
import com.amazonaws.proserve.workshop.serde.JsonDeserializationSchema;
import com.amazonaws.proserve.workshop.serde.JsonSerializationSchema;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.collections.functors.TruePredicate;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.amazonaws.proserve.workshop.process.model.AttackResult;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.functions.AggregateFunction;
import android.content.Context;

/**
 * Entry point class. Defines and parses CLI arguments, instantiate top level
 * classes and starts the job.
 */

@CommandLine.Command(name = "FreeThrowPrediction", mixinStandardHelpOptions = true, description = "Predict the outcome of Free Throw event during the game")
@Slf4j
public class AnomalyDetection implements Runnable {
    @CommandLine.Option(names = { "-g", "--config-group" }, description = "Configuration Group")
    private static String propertyGroupId = "AnomalyDetection";

    @CommandLine.Option(names = { "-f", "--config-file" }, description = "Configuration File")
    private static String propertyFile = "";

    public static void main(String[] args) {
        new CommandLine(new AnomalyDetection()).execute(args);
    }

    @Override
    public void run() {
        try {

            Properties jobProps = getProps(propertyGroupId, propertyFile);

            String sourceTopic = getProperty(jobProps, "sourceTopic", "");
            String sourceBootstrapServer = getProperty(jobProps, "sourceBootstrapServer", "");
            String sinkTopic = getProperty(jobProps, "sinkTopic", "");
            String sinkBootstrapServer = getProperty(jobProps, "sinkBootstrapServer", "");
            log.info("Flink Job properties map: sourceTopic {} sinkTopic {} sourceBootstrapServer {} sinkBootstrapServer {}", sourceTopic,  sinkTopic, sourceBootstrapServer, sinkBootstrapServer);

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            
            // Configure Flink web dashboard port for local environment only
            if (env instanceof org.apache.flink.streaming.api.environment.LocalStreamEnvironment) {
                org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
                // config.setString("rest.port", "53374");
                env.configure(config);
                env.setParallelism(4);
            }
            Properties kafkaProps = new Properties();
            kafkaProps.setProperty("security.protocol", "SASL_SSL");
            kafkaProps.setProperty("sasl.mechanism", "AWS_MSK_IAM");
            kafkaProps.setProperty("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
            kafkaProps.setProperty("sasl.client.callback.handler.class",
                    "software.amazon.msk.auth.iam.IAMClientCallbackHandler");


            String initpos = getProperty(jobProps, "initpos", "EARLIEST");
            OffsetsInitializer startingOffsets;
            if ("LATEST".equals(initpos)) {
                startingOffsets = OffsetsInitializer.latest();
            } else if ("EARLIEST".equals(initpos)) {
                startingOffsets = OffsetsInitializer.earliest();
            } else {
                if (StringUtils.isBlank(initpos)) {
                    throw new IllegalArgumentException(
                            "Please set value for initial position to be one of LATEST, EARLIEST or use a timestamp for TIMESTAMP position");
                }
                startingOffsets = OffsetsInitializer.timestamp(Long.parseLong(initpos));
            }

            final KafkaSource<Event> dataSource = KafkaSource.<Event>builder().setProperties(kafkaProps)
                    .setBootstrapServers(sourceBootstrapServer).setGroupId("AnomalyDetectorApp")
                    .setTopics(sourceTopic).setStartingOffsets(startingOffsets)
                    .setValueOnlyDeserializer(JsonDeserializationSchema.forSpecific(Event.class)).build();

            final DataStream<Event> stream = env.fromSource(dataSource, 
                    WatermarkStrategy.<Event>forMonotonousTimestamps()
                            .withTimestampAssigner((event, timestamp) -> event.getCalculatedEventTime().toEpochMilli()), 
                    "Source");

            // Calculate rolling average packets per destination IP
            DataStream<Event> enrichedStream = stream
                    .keyBy(Event::getIpDst)
                    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(1)))
                    .aggregate(new AverageAggregate());

            Pattern<Event, ?> pattern = Pattern.<Event>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(SimpleCondition.of(event -> event.getPackets() < 10))
                .times(2, 30)
                .followedBy("normal")
                .where(SimpleCondition.of(event -> event.getPackets() > 10))
                .times(1)
                .within(Time.minutes(1))
                .greedy();

            // Apply CEP pattern
            PatternStream<Event> patternStream = CEP.pattern(
                    enrichedStream.keyBy(Event::getIpDst), pattern)
                    .inProcessingTime();
                    //.inEventTime();

            // Extract attack results
            DataStream<AttackResult> attackResults = patternStream.select(
                    (Map<String, List<Event>> pattern1) -> {
                        List<Event> events = pattern1.get("start");
                        // Only enable this when debugging locally.
                        log.info("Anomalous pattern is found: number of events in a pattern: {}", events.size());
                        Event first = events.get(0);
                        Event last = events.get(events.size() - 1);
                        
                        double avgFragmentSize = events.stream()
                                .mapToDouble(e -> (double) e.getBytes() / e.getPackets())
                                .average().orElse(0.0);
                        
                        double avgPackets = events.stream()
                                .mapToDouble(Event::getAvgPackets)
                                .average().orElse(0.0);
                        
                        return AttackResult.builder()
                                .attackStartTime(first.getCalculatedEventTime())
                                .attackEndTime(Instant.ofEpochMilli(last.getTsEnd().longValue()))
                                .attackerId(first.getIpSrc())
                                .targetIp(first.getIpDst())
                                .fragmentCount((long) events.size())
                                .avgPackets(avgPackets)
                                .avgFragmentSize(avgFragmentSize)
                                .sizeReductionPercent((avgPackets - avgFragmentSize) / avgPackets * 100)
                                .build();
                    });

            // Create Kafka sink
            KafkaSink<AttackResult> sink = KafkaSink.<AttackResult>builder()
                    .setBootstrapServers(sinkBootstrapServer)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(sinkTopic)
                            .setValueSerializationSchema(JsonSerializationSchema.forSpecific(AttackResult.class))
                            .build())
                    .setKafkaProducerConfig(kafkaProps)
                    .build();

            attackResults.sinkTo(sink).name("Sink");
            
            env.execute("Anomaly Detection");
        } catch (Exception ex) {
            log.error("Failed to initialize job because of exception: {}, stack: {}", ex, ex.getStackTrace());
            throw new RuntimeException(ex);
        }
    }

    protected static Properties getProps(String propertyGroupId, String configFile) throws IOException {
        if (!configFile.isEmpty()) {
            log.debug("Load AppProperties from provided file: {}", configFile);
            Properties props = new Properties();
            try (java.io.FileInputStream fis = new java.io.FileInputStream(configFile)) {
                props.load(fis);
            }
            return props;
        } else {
            Map<String, Properties> appConfigs = KinesisAnalyticsRuntime.getApplicationProperties();
            Properties props = appConfigs.get(propertyGroupId);
            if (props == null || props.isEmpty()) {
                throw new IllegalArgumentException(
                        "No such property group found or group have no properties, group id: " + propertyGroupId);
            }
            return props;
        }
    }

    protected static String getProperty(Properties properties, String name, String defaultValue) {
        String value = properties.getProperty(name);
        if (StringUtils.isBlank(value)) {
            value = defaultValue;
        }
        return value;
    }
    
    public static class AverageAggregate implements AggregateFunction<Event, AverageAggregate.Accumulator, Event> {
        
        public static class Accumulator {
            public double sum = 0.0;
            public int count = 0;
            public Event lastEvent;
        }
        
        @Override
        public Accumulator createAccumulator() {
            return new Accumulator();
        }
        
        @Override
        public Accumulator add(Event event, Accumulator acc) {
            acc.sum += event.getPackets();
            acc.count++;
            acc.lastEvent = event;
            return acc;
        }
        
        @Override
        public Event getResult(Accumulator acc) {
            double avgPackets = acc.count > 0 ? acc.sum / acc.count : 0.0;
            acc.lastEvent.setAvgPackets(avgPackets);
            return acc.lastEvent;
        }
        
        @Override
        public Accumulator merge(Accumulator acc1, Accumulator acc2) {
            acc1.sum += acc2.sum;
            acc1.count += acc2.count;
            return acc1;
        }
    }
}