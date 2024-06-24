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

import com.amazonaws.proserve.workshop.process.AsyncPrediction;
import com.amazonaws.proserve.workshop.process.EventsAggregator;
import com.amazonaws.proserve.workshop.serde.JsonDeserializationSchema;
import com.amazonaws.proserve.workshop.process.model.Event;
import com.amazonaws.proserve.workshop.serde.JsonSerializationSchema;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

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
            Properties kafkaProps = new Properties();
            kafkaProps.setProperty("security.protocol", "SASL_SSL");
            kafkaProps.setProperty("sasl.mechanism", "AWS_MSK_IAM");
            kafkaProps.setProperty("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
            kafkaProps.setProperty("sasl.client.callback.handler.class",
                    "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

            Properties jobProps = getProps(propertyGroupId, propertyFile);

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

            String sourceTopic = getProperty(jobProps, "sourceTopic", "");
            String sourceBootstrapServer = getProperty(jobProps, "sourceBootstrapServer", "");
            final KafkaSource<Event> dataSource = KafkaSource.<Event>builder().setProperties(kafkaProps)
                    .setBootstrapServers(sourceBootstrapServer).setGroupId("AnomalyDetectorApp")
                    .setTopics(sourceTopic).setStartingOffsets(startingOffsets)
                    .setValueOnlyDeserializer(JsonDeserializationSchema.forSpecific(Event.class)).build();

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            final DataStream<Event> stream = env.fromSource(dataSource, WatermarkStrategy.noWatermarks(), "Source");

            String sageMakerEndpoint = getProperty(jobProps, "sagemakerEndpoint", "");
            double threshold = getProperty(jobProps, "threshold", 3.0);
            int maxEvents = getProperty(jobProps, "maxEvents", 100);
            int freqSecs = getProperty(jobProps, "freqSecs", 5);
            DataStream<Event> predictions = AsyncDataStream.unorderedWait(
                    stream.keyBy(Event::getWriterId).process(new EventsAggregator(maxEvents, freqSecs)),
                    new AsyncPrediction(sageMakerEndpoint, threshold), 10_000, TimeUnit.MILLISECONDS, 100);

            String sinkTopic = getProperty(jobProps, "sinkTopic", "");
            String sinkBootstrapServer = getProperty(jobProps, "sinkBootstrapServer", "");
            KafkaSink<Event> sink = KafkaSink.<Event>builder().setKafkaProducerConfig(kafkaProps)
                    .setBootstrapServers(sinkBootstrapServer)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(sinkTopic)
                            .setValueSerializationSchema(JsonSerializationSchema.forSpecific(Event.class)).build())
                    .build();
            predictions.sinkTo(sink).name("Sink");

            env.execute();
        } catch (Exception ex) {
            log.error("Failed to initialize job because of exception: {}, stack: {}", ex, ex.getStackTrace());
            throw new RuntimeException(ex);
        }
    }

    protected static Properties getProps(String propertyGroupId, String configFile) throws IOException {
        Map<String, Properties> appConfigs;

        if (!configFile.isEmpty()) {
            log.debug("Load AppProperties from provided file: {}", configFile);
            appConfigs = KinesisAnalyticsRuntime.getApplicationProperties(configFile);
        } else {
            appConfigs = KinesisAnalyticsRuntime.getApplicationProperties();
        }

        Properties props = appConfigs.get(propertyGroupId);
        if (props == null || props.isEmpty()) {
            throw new IllegalArgumentException(
                    "No such property group found or group have no properties, group id: " + propertyGroupId);
        }

        return props;
    }

    protected static String getProperty(Properties properties, String name, String defaultValue) {
        String value = properties.getProperty(name);
        if (StringUtils.isBlank(value)) {
            value = defaultValue;
        }
        return value;
    }

    protected static double getProperty(Properties properties, String name, double defaultValue) {
        String value = properties.getProperty(name);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    protected static int getProperty(Properties properties, String name, int defaultValue) {
        String value = properties.getProperty(name);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}