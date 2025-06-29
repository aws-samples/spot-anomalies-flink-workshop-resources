%flink.ssql(type=update, interpolate=true)
%flink.ssql(type=update, interpolate=true)
DROP TABLE IF EXISTS log_events_input;

CREATE TABLE log_events_input (
event_type VARCHAR,
ip_src VARCHAR,
ip_dst VARCHAR,
timestamp_start VARCHAR,
writer_id VARCHAR,
packets INT,
`bytes` INT,
`text` VARCHAR,
event_time AS PROCTIME()
) WITH (
'connector'= 'kafka',
'topic' = 'flow-log-ingest',
'properties.bootstrap.servers' = '{brokers}',
'format'= 'json',
'properties.group.id' = 'group-1',
'scan.startup.mode' = 'latest-offset',
'json.timestamp-format.standard'= 'ISO-8601',
'properties.security.protocol' = 'SASL_SSL',
'properties.sasl.mechanism' = 'AWS_MSK_IAM',
'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
);

WITH enriched_events AS (
SELECT *,
           AVG(CAST(packets AS DOUBLE)) OVER (
               PARTITION BY ip_dst
               ORDER BY event_time 
               RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
           ) as avg_packets
    FROM log_events_input
)
SELECT 
        attack_start_time,
        attack_end_time,
        attacker_ip,
        target_ip,
        fragment_count,
        avg_fragment_size,
        (avg_packets - avg_fragment_size) / avg_packets * 100 as size_reduction_percent
    FROM enriched_events
    MATCH_RECOGNIZE (
        PARTITION BY ip_dst
        ORDER BY event_time
        MEASURES
            FIRST(A.event_time) as attack_start_time,
            LAST(A.event_time) as attack_end_time,
            FIRST(A.ip_src) as attacker_ip,
            FIRST(A.ip_dst) as target_ip,
            FIRST(A.avg_packets) as avg_packets,
            COUNT(*) as fragment_count,
            AVG(CAST(A.`bytes` AS DOUBLE) / CAST(A.packets AS DOUBLE)) as avg_fragment_size
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (A{20,} B)
        WITHIN INTERVAL '2' MINUTE
        DEFINE
            A AS (
               CAST(A.packets AS DOUBLE) < (A.avg_packets * 0.1)
            )
    );

