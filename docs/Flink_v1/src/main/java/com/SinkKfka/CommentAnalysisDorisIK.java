package com.SinkKfka;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import util.IKAnalyzerUtil;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class CommentAnalysisDorisIK {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStateBackend(new HashMapStateBackend());
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/flink-checkpoints/");

        // --- SQL Server CDC Source ---
        Properties debeziumProps = new Properties();
        debeziumProps.put("decimal.handling.mode", "string");

        DebeziumSourceFunction<String> sqlServerSource =
                SqlServerSource.<String>builder()
                        .hostname("192.168.200.32")
                        .port(1433)
                        .database("realtime_v3")
                        .tableList("dbo.product_comments")
                        .username("sa")
                        .password("wjh123,./")
                        .debeziumProperties(debeziumProps)
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();

        DataStreamSource<String> sourceStream = env.addSource(sqlServerSource);

        // --- 读取敏感词 ---
        List<String> p0Words = readSensitiveWords("D:\\soft2\\stream-bda-prod\\Flink_v1\\src\\main\\java\\com\\SinkKfka\\suspected-sensitive-words.txt");
        List<String> p1Words = readSensitiveWords("D:\\soft2\\stream-bda-prod\\Flink_v1\\src\\main\\java\\com\\SinkKfka\\p1-sensitive-words.txt");

        System.out.println("P0敏感词数量: " + p0Words.size());
        System.out.println("P1敏感词数量: " + p1Words.size());

        List<String> allWords = new ArrayList<>();
        allWords.addAll(p0Words);
        allWords.addAll(p1Words);
        IKAnalyzerUtil.preloadSensitiveWords(allWords);

        // --- 创建敏感词广播流 ---
        List<SensitiveWord> sensitiveList = new ArrayList<>();
        p0Words.forEach(word -> sensitiveList.add(new SensitiveWord(word, "P0")));
        p1Words.forEach(word -> sensitiveList.add(new SensitiveWord(word, "P1")));

        MapStateDescriptor<String, SensitiveWord> sensitiveStateDescriptor =
                new MapStateDescriptor<>("sensitiveWords", Types.STRING, Types.POJO(SensitiveWord.class));

        BroadcastStream<SensitiveWord> broadcastStream = env.fromCollection(sensitiveList)
                .broadcast(sensitiveStateDescriptor);

        // --- 数据处理 ---
        KeyedStream<String, String> keyedStream = sourceStream.keyBy(json -> {
            try {
                JSONObject obj = JSON.parseObject(json);
                JSONObject after = obj.getJSONObject("after");
                return after != null ? after.getString("user_id") : "unknown";
            } catch (Exception e) {
                return "unknown";
            }
        });

        SingleOutputStreamOperator<CommentResult> processedStream = keyedStream
                .connect(broadcastStream)
                .process(new CommentLevelProcessor())
                .name("comment-level-processor");

        // --- Kafka Sink ---
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "192.168.200.32:9092");

        FlinkKafkaProducer<CommentResult> kafkaProducer = new FlinkKafkaProducer<>(
                "sqlserver-comments-topic",
                new CommentResultKafkaSerializer(),
                kafkaProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );

        processedStream.addSink(kafkaProducer);

        env.execute("SQLServer Comment to Kafka with Full Fields");
    }

    private static List<String> readSensitiveWords(String path) {
        try {
            return Files.lines(Paths.get(path))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            System.err.println("读取敏感词失败: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    // ====================== 内部类 ======================

    public static class SensitiveWord {
        private String word;
        private String level;
        public SensitiveWord() {}
        public SensitiveWord(String word, String level) { this.word = word; this.level = level; }
        public String getWord() { return word; }
        public void setWord(String word) { this.word = word; }
        public String getLevel() { return level; }
        public void setLevel(String level) { this.level = level; }
    }

    public static class CommentResult {
        private String userid;
        private String orderid;
        private String username;
        private String comment;
        private int isBlack;
        private String commentLevel;
        private List<String> sensitiveWords;
        private Long timestamp;
        private String productId;     // 新增字段
        private String productName;   // 新增字段

        public CommentResult() {}

        public CommentResult(String userid, String orderid, String username, String comment,
                             int isBlack, String commentLevel, List<String> sensitiveWords,
                             String productId, String productName) {
            this.userid = userid;
            this.orderid = orderid;
            this.username = username;
            this.comment = comment;
            this.isBlack = isBlack;
            this.commentLevel = commentLevel;
            this.sensitiveWords = sensitiveWords;
            this.timestamp = System.currentTimeMillis();
            this.productId = productId;
            this.productName = productName;
        }

        // --- Getter & Setter ---
        public String getUserid() { return userid; }
        public void setUserid(String userid) { this.userid = userid; }
        public String getOrderid() { return orderid; }
        public void setOrderid(String orderid) { this.orderid = orderid; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getComment() { return comment; }
        public void setComment(String comment) { this.comment = comment; }
        public int getIsBlack() { return isBlack; }
        public void setIsBlack(int isBlack) { this.isBlack = isBlack; }
        public String getCommentLevel() { return commentLevel; }
        public void setCommentLevel(String commentLevel) { this.commentLevel = commentLevel; }
        public List<String> getSensitiveWords() { return sensitiveWords; }
        public void setSensitiveWords(List<String> sensitiveWords) { this.sensitiveWords = sensitiveWords; }
        public Long getTimestamp() { return timestamp; }
        public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
        public String getProductId() { return productId; }
        public void setProductId(String productId) { this.productId = productId; }
        public String getProductName() { return productName; }
        public void setProductName(String productName) { this.productName = productName; }
    }

    public static class CommentLevelProcessor extends KeyedBroadcastProcessFunction<String, String, SensitiveWord, CommentResult> {
        private MapStateDescriptor<String, SensitiveWord> stateDesc =
                new MapStateDescriptor<>("sensitiveWords", Types.STRING, Types.POJO(SensitiveWord.class));

        @Override
        public void processElement(String json, ReadOnlyContext ctx, Collector<CommentResult> out) throws Exception {
            JSONObject obj = JSON.parseObject(json);
            JSONObject after = obj.getJSONObject("after");
            if (after == null) return;

            String commentText = after.getString("comment");
            if (commentText == null) commentText = "";

            ReadOnlyBroadcastState<String, SensitiveWord> broadcastState = ctx.getBroadcastState(stateDesc);
            List<String> p0Words = new ArrayList<>();
            List<String> p1Words = new ArrayList<>();
            for (Map.Entry<String, SensitiveWord> entry : broadcastState.immutableEntries()) {
                if ("P0".equals(entry.getValue().getLevel())) p0Words.add(entry.getKey());
                else if ("P1".equals(entry.getValue().getLevel())) p1Words.add(entry.getKey());
            }

            Map<String, List<String>> detection = IKAnalyzerUtil.detectSensitiveWordsWithLevel(commentText, p0Words, p1Words);
            List<String> matched = new ArrayList<>();
            matched.addAll(detection.get("P0"));
            matched.addAll(detection.get("P1"));

            String level = detection.get("P0").isEmpty() ? (detection.get("P1").isEmpty() ? "P2" : "P1") : "P0";

            // 构建 CommentResult（包含新增字段 productId, productName）
            CommentResult result = new CommentResult(
                    after.getString("user_id"),
                    after.getString("order_id"),
                    after.getString("user_name"),
                    commentText,
                    "P0".equals(level) ? 1 : 0,
                    level,
                    matched,
                    after.getString("product_id"),
                    after.getString("product_name")
            );

            out.collect(result);
        }

        @Override
        public void processBroadcastElement(SensitiveWord word, Context ctx, Collector<CommentResult> out) throws Exception {
            ctx.getBroadcastState(stateDesc).put(word.getWord(), word);
        }
    }

    public static class CommentResultKafkaSerializer implements KafkaSerializationSchema<CommentResult> {
        @Override
        public ProducerRecord<byte[], byte[]> serialize(CommentResult element, @Nullable Long timestamp) {
            String json = JSON.toJSONString(element);
            return new ProducerRecord<>("sqlserver-comments-topic",
                    element.getUserid() != null ? element.getUserid().getBytes(StandardCharsets.UTF_8) : null,
                    json.getBytes(StandardCharsets.UTF_8));
        }
    }
}
