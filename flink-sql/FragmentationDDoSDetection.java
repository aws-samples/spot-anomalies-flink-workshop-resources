import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class FragmentationDDoSDetection {
    
    public static class LogEvent {
        public String eventType;
        public String ipSrc;
        public String ipDst;
        public long timestampStart;
        public String writerId;
        public int packets;
        public int bytes;
        public String text;
        
        public double getPacketSize() {
            return packets > 0 ? (double) bytes / packets : 0;
        }
    }
    
    public static class AttackResult {
        public long attackStartTime;
        public long attackEndTime;
        public String attackerIp;
        public String targetIp;
        public int fragmentCount;
        public int totalPackets;
        public double avgFragmentSize;
        public double sizeReductionPercent;
    }
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Assume log_events_input DataStream is created from source
        DataStream<LogEvent> logEvents = env.fromSource(/* your source */, 
            WatermarkStrategy.<LogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> event.timestampStart),
            "log-events");
        
        // Define CEP pattern
        Pattern<LogEvent, ?> pattern = Pattern.<LogEvent>begin("start")
            .where(new SimpleCondition<LogEvent>() {
                @Override
                public boolean filter(LogEvent event) {
                    return event.getPacketSize() < 64;
                }
            })
            .timesOrMore(20)
            .followedBy("end")
            .within(Time.minutes(2));
        
        // Apply pattern to keyed stream
        PatternStream<LogEvent> patternStream = CEP.pattern(
            logEvents.keyBy(event -> event.ipSrc), 
            pattern
        );
        
        // Select matches and create results
        DataStream<AttackResult> attacks = patternStream.select(
            new PatternSelectFunction<LogEvent, AttackResult>() {
                @Override
                public AttackResult select(Map<String, List<LogEvent>> pattern) {
                    List<LogEvent> startEvents = pattern.get("start");
                    
                    AttackResult result = new AttackResult();
                    result.attackStartTime = startEvents.get(0).timestampStart;
                    result.attackEndTime = startEvents.get(startEvents.size() - 1).timestampStart;
                    result.attackerIp = startEvents.get(0).ipSrc;
                    result.targetIp = startEvents.get(0).ipDst;
                    result.fragmentCount = startEvents.size();
                    result.totalPackets = startEvents.stream().mapToInt(e -> e.packets).sum();
                    result.avgFragmentSize = startEvents.stream()
                        .mapToDouble(LogEvent::getPacketSize).average().orElse(0);
                    result.sizeReductionPercent = (200 - result.avgFragmentSize) / 200 * 100;
                    
                    return result;
                }
            }
        );
        
        // Filter results where avg fragment size < 64
        attacks.filter(attack -> attack.avgFragmentSize < 64)
               .print();
        
        env.execute("Fragmentation DDoS Detection");
    }
}