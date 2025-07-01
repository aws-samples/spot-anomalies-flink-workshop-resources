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

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

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

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());

            tableEnv.executeSql("CREATE TABLE log_events_input (\n" +
                    "event_type VARCHAR,\n" +
                    "ip_src VARCHAR,\n" +
                    "ip_dst VARCHAR,\n" +
                    "timestamp_start BIGINT,\n" +
                    "timestamp_end BIGINT,\n" +
                    "writer_id VARCHAR,\n" +
                    "packets INT,\n" +
                    "`bytes` INT,\n" +
                    "`text` VARCHAR,\n" +
                    "event_time AS TO_TIMESTAMP_LTZ(timestamp_start, 3),\n" +
                    "WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND\n" +
                    ") WITH (\n" +
                    "'connector'= 'kafka',\n" +
                    "'topic' = '" + sourceTopic + "',\n" +
                    "'properties.bootstrap.servers' = '" + sourceBootstrapServer + "',\n" +
                    "'format'= 'json',\n" +
                    "'properties.group.id' = 'group-1',\n" +
                    "'scan.startup.mode' = 'latest-offset',\n" +
                    "'properties.security.protocol' = 'SASL_SSL',\n" +
                    "'properties.sasl.mechanism' = 'AWS_MSK_IAM',\n" +
                    "'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',\n" +
                    "'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler'\n" +
                    ")\n");

            tableEnv.executeSql("CREATE TABLE log_events_output (\n" +
                    "attack_start_time TIMESTAMP_LTZ(3),\n" +
                    "attack_end_time TIMESTAMP_LTZ(3),\n" +
                    "attacker_id VARCHAR,\n" +
                    "target_ip VARCHAR,\n" +
                    "fragment_count BIGINT,\n" +
                    "avg_packets DOUBLE,\n" +
                    "avg_fragment_size DOUBLE,\n" +
                    "size_reduction_percent DOUBLE\n" +
                    ") WITH (\n" +
                    "'connector'= 'kafka',\n" +
                    "'topic' = '" + sinkTopic + "',\n" +
                    "'properties.bootstrap.servers' = '" + sinkBootstrapServer + "',\n" +
                    "'format'= 'json',\n" +
                    "'properties.group.id' = 'group-2',\n" +
                    "'scan.startup.mode' = 'latest-offset',\n" +
                    "'properties.security.protocol' = 'SASL_SSL',\n" +
                    "'properties.sasl.mechanism' = 'AWS_MSK_IAM',\n" +
                    "'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',\n" +
                    "'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler'\n" +
                    ")");
            StreamStatementSet statementSet = tableEnv.createStatementSet();
            statementSet.addInsertSql("insert into log_events_output \n" +
                    "WITH enriched_events AS (\n" +
                    "SELECT *,\n" +
                    "           AVG(CAST(packets AS DOUBLE)) OVER (\n" +
                    "               PARTITION BY ip_dst\n" +
                    "               ORDER BY event_time \n" +
                    "               RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW\n" +
                    "           ) as avg_packets\n" +
                    "    FROM log_events_input\n" +
                    ")\n" +
                    "SELECT \n" +
                    "        attack_start_time,\n" +
                    "        attack_end_time,\n" +
                    "        attacker_ip,\n" +
                    "        target_ip,\n" +
                    "        fragment_count,\n" +
                    "        avg_packets,\n" +
                    "        avg_fragment_size,\n" +
                    "        (avg_packets - avg_fragment_size) / avg_packets * 100 as size_reduction_percent\n" +
                    "    FROM enriched_events\n" +
                    "    MATCH_RECOGNIZE (\n" +
                    "        PARTITION BY ip_dst\n" +
                    "        ORDER BY event_time\n" +
                    "        MEASURES\n" +
                    "            FIRST(A.event_time) as attack_start_time,\n" +
                    "            LAST(TO_TIMESTAMP_LTZ(A.timestamp_end, 3)) as attack_end_time,\n" +
                    "            FIRST(A.ip_src) as attacker_ip,\n" +
                    "            FIRST(A.ip_dst) as target_ip,\n" +
                    "            AVG(A.avg_packets) as avg_packets,\n" +
                    "            COUNT(*) as fragment_count,\n" +
                    "            AVG(CAST(A.`bytes` AS DOUBLE) / CAST(A.packets AS DOUBLE)) as avg_fragment_size\n" +
                    "        ONE ROW PER MATCH\n" +
                    "        AFTER MATCH SKIP PAST LAST ROW\n" +
                    "        PATTERN (A{20,} B)\n" +
                    "        WITHIN INTERVAL '2' MINUTE\n" +
                    "        DEFINE\n" +
                    "            A AS (\n" +
                    "               CAST(A.packets AS DOUBLE) < (A.avg_packets * 0.1)\n" +
                    "            )\n" +
                    "    )");
            statementSet.execute();
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
}