package com;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.ConfigUtils;

import java.util.Properties;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

class CommentProcessorCDC {

    private static final String FLINK_UID_VERSION = "_v1";
    // Kafka topic
    private static final String KAFKA_TOPIC = "realtime_v3_comment_cdc";

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置环境 - 使用本地文件系统
        configureEnvironment(env);

        Properties debeziumProperties = new Properties();

        // Debezium 配置
        debeziumProperties.put("connect.timeout.ms", 10000);
        debeziumProperties.put("request.timeout.ms", 15000);
        debeziumProperties.put("heartbeat.interval.ms", 10000);
        debeziumProperties.put("snapshot.mode", "initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.put("snapshot.locking.mode", "none");
        debeziumProperties.put("snapshot.fetch.size", 200);
        debeziumProperties.put("snapshot.isolation.mode", "snapshot");
        debeziumProperties.put("signal.data.collection", "dbo.product_comments");
        debeziumProperties.put("decimal.handling.mode", "double");
        debeziumProperties.put("binary.handling.mode", "base64");

        DataStreamSource<String> dataStreamSource = env.addSource(
                SqlServerSource.<String>builder()
                        .hostname("192.168.200.32")
                        .port(1433)
                        .username("sa")
                        .password("wjh123,./")
                        .database("realtime_v3")
                        .tableList("dbo.product_comments")
                        .debeziumProperties(debeziumProperties)
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build(),
                "_comment_cdc_source"
        );

        // 解析JSON数据并添加调试信息
        SingleOutputStreamOperator<JSONObject> converStr2JsonDs = dataStreamSource
                .map(jsonStr -> {
                    try {
                        System.out.println("=== 原始JSON字符串 ===");
                        System.out.println(jsonStr);
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        System.out.println("=== 解析后的JSON对象 ===");
                        System.out.println(jsonObj.toJSONString());
                        System.out.println("====================\n");
                        return jsonObj;
                    } catch (Exception e) {
                        System.err.println("JSON解析失败: " + e.getMessage());
                        System.err.println("原始数据: " + jsonStr);
                        // 返回一个空的JSON对象避免中断流程
                        return new JSONObject();
                    }
                })
                .uid("converStr2JsonDs" + FLINK_UID_VERSION)
                .name("converStr2JsonDs");

        // 处理评论数据，输出为 String（JSON文本）
        SingleOutputStreamOperator<String> resultDs = converStr2JsonDs
                .map(jsonNode -> processCommentData(jsonNode))
                .uid("processCommentData" + FLINK_UID_VERSION)
                .name("processCommentData");

        // *********************************
        //  ⭐ 写入 Kafka Sink ⭐
        // *********************************
        String kafkaBootstrap = ConfigUtils.getString("kafka.bootstrap.servers");
        if (kafkaBootstrap == null || kafkaBootstrap.trim().isEmpty()) {
            // fallback 默认值（如果没有在 ConfigUtils 配置）
            kafkaBootstrap = "192.168.200.32:9092";
        }

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(KAFKA_TOPIC)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 发送到 Kafka
        resultDs.sinkTo(kafkaSink)
                .uid("sinkToKafka" + FLINK_UID_VERSION)
                .name("sinkToKafka");

        // 控制台打印（开发调试用）
        resultDs.print("Comment Process Result: ->");

        System.out.println("启动 SQL Server 评论 CDC → Kafka 同步任务...");
        System.out.println("监控表: dbo.product_comments"); // 🔴 修正：原错误为 oms_order_user_comment，与实际监控表一致
        System.out.println("目标 Topic: " + KAFKA_TOPIC);
        System.out.println("kafka.bootstrap.servers = " + kafkaBootstrap);

        try {
            env.execute("CommentProcessorCDC");
        } catch (Exception e) {
            System.err.println("任务执行失败: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * 配置环境 - 使用本地文件系统
     */
    private static void configureEnvironment(StreamExecutionEnvironment env) {
        // 使用本地文件系统进行 checkpoint
        String checkpointPath = "file:///tmp/flink-checkpoints/comment-cdc";

        // 明确设置 checkpoint 存储为本地文件系统
        env.getCheckpointConfig().setCheckpointStorage(checkpointPath);

        // 设置 checkpoint 间隔
        env.enableCheckpointing(30000); // 30秒

        System.out.println("Checkpoint 配置: " + checkpointPath);

        // 设置并行度
        env.setParallelism(1);
    }

    /**
     * 处理评论数据 - 核心修改：字段名 user_comment → review_text
     */
    private static String processCommentData(JSONObject jsonNode) {
        try {
            // 检查JSON节点是否为空
            if (jsonNode == null || jsonNode.isEmpty()) {
                return "处理失败: 空数据";
            }

            // 获取操作类型
            String op = jsonNode.getString("op");
            if (op == null) {
                return "处理失败: 缺少操作类型(op)";
            }

            System.out.println("处理操作类型: " + op);

            // 根据操作类型处理数据
            JSONObject data = null;
            if ("c".equals(op) || "u".equals(op) || "r".equals(op)) {
                // 创建、更新、读取操作使用 after 数据
                data = jsonNode.getJSONObject("after");
            } else if ("d".equals(op)) {
                // 删除操作使用 before 数据
                data = jsonNode.getJSONObject("before");
                if (data != null) {
                    JSONObject result = new JSONObject();
                    result.put("operation", "DELETE");
                    result.put("deleted_data", data);
                    result.put("process_time", System.currentTimeMillis());
                    return result.toString();
                }
            }

            if (data == null || data.isEmpty()) {
                return "跳过空数据记录，操作: " + op;
            }

            // 🔴 核心修改1：字段名 user_comment → review_text（对应实际数据中的评论字段）
            Object userIdObj = data.get("user_id");
            Object reviewTextObj = data.get("review_text"); // 替换 user_comment 为 review_text
            Object orderIdObj = data.get("order_id");

            // 🔴 核心修改2：必要字段检查 - 校验 review_text 是否存在（替换原 user_comment）
            if (userIdObj == null || reviewTextObj == null || orderIdObj == null) {
                System.out.println("缺失必要字段 - user_id: " + userIdObj +
                        ", review_text: " + reviewTextObj + ", order_id: " + orderIdObj); // 日志字段名同步修改
                return "跳过缺失必要字段的记录";
            }

            // 转换字段值
            String userId = userIdObj.toString();
            String commentContent = reviewTextObj.toString(); // 评论内容从 review_text 提取
            String orderId = orderIdObj.toString();

            // 跳过空评论（现在校验的是 review_text 的值）
            if (commentContent == null || commentContent.trim().isEmpty()) {
                return "跳过空评论: 订单=" + orderId;
            }

            System.out.println("\n=== 开始处理评论 订单: " + orderId + " ===");
            System.out.println("用户: " + userId);
            System.out.println("订单: " + orderId);
            System.out.println("评论内容: " + (commentContent.length() > 100 ?
                    commentContent.substring(0, 100) + "..." : commentContent));

            // 解析金额字段（若数据中无 total_amount，返回 null 不影响后续逻辑）
            Double totalAmount = parseAmountField(data);
            if (totalAmount == null) {
                totalAmount = extractAmountFromComment(commentContent);
            }

            // 获取评论时间相关字段
            String commentTime = getStringSafe(data, "ds");
            Long timestamp = convertToLong(data.get("ts"));
            Long createdTime = convertToLong(data.get("created_time"));

            // 处理金额格式
            int totalAmountInt = 0;
            if (totalAmount != null) {
                totalAmountInt = totalAmount.intValue();
            } else {
                totalAmountInt = extractAmountDirectly(commentContent);
            }
            System.out.println("💰 最终金额: " + totalAmountInt);

            // 敏感词检测（基于 review_text 内容）
            JSONObject sensitiveResult = detectSensitiveWords(commentContent);

            // 构建结果JSON - 对外输出字段名可保留 user_comment（也可改为 review_text，根据需求调整）
            JSONObject result = new JSONObject();
            result.put("order_id", orderId);
            result.put("user_id", userId);
            result.put("user_comment", commentContent); // 对外输出字段名可保留 user_comment，值来自 review_text
            // 若需对外统一为 review_text，可改为：result.put("review_text", commentContent);
            result.put("ds", commentTime != null ? commentTime : "");
            result.put("ts", timestamp != null ? timestamp.toString() : String.valueOf(System.currentTimeMillis()));
            result.put("created_time", createdTime != null ? createdTime.toString() : String.valueOf(System.currentTimeMillis()));
            result.put("is_insulting", sensitiveResult.getBoolean("isSensitive"));
            result.put("db", "realtime_v3");
            result.put("schema", "dbo");
            result.put("table", "product_comments"); // 🔴 修正：与实际监控表一致，原错误为 oms_order_user_comment
            result.put("operation", op);
            result.put("sensitive_level", sensitiveResult.getString("level"));
            result.put("is_blocked", sensitiveResult.getBoolean("isSensitive"));
            result.put("blacklist_duration_days", sensitiveResult.getInteger("banDays"));
            result.put("triggered_keyword", sensitiveResult.getString("triggeredKeyword"));
            result.put("keyword_source", "SENSITIVE_WORDS");
            result.put("total_amount", totalAmountInt);
            result.put("process_time", System.currentTimeMillis());

            // 输出处理结果
            if (sensitiveResult.getBoolean("isSensitive")) {
                System.out.println("🚨 敏感评论警报 - 用户: " + userId +
                        ", 级别: " + sensitiveResult.getString("level") +
                        ", 封禁: " + sensitiveResult.getInteger("banDays") + "天" +
                        ", 金额: " + totalAmountInt +
                        ", 触发关键词: " + sensitiveResult.getString("triggeredKeyword"));
            } else {
                System.out.println("✅ 正常评论 - 用户: " + userId +
                        ", 金额: " + totalAmountInt +
                        ", 订单: " + orderId);
            }
            System.out.println("=== 结束处理评论 订单: " + orderId + " ===\n");

            return result.toString();

        } catch (Exception e) {
            System.err.println("处理评论数据失败: " + e.getMessage());
            e.printStackTrace();
            // 返回原始数据以便调试
            return "处理失败 - 异常: " + e.getMessage() + ", 原始数据: " +
                    (jsonNode != null ? jsonNode.toString() : "null");
        }
    }

    /**
     * 简化的敏感词检测
     */
    private static JSONObject detectSensitiveWords(String commentContent) {
        // 调用SensitiveWordDetector的检测方法，获取专业检测结果
        SensitiveWordDetector.SensitiveResult detectorResult = SensitiveWordDetector.detect(commentContent);

        // 构建返回的JSONObject，映射检测结果字段
        JSONObject result = new JSONObject();
        result.put("isSensitive", detectorResult.isSensitive); // 是否敏感
        result.put("level", detectorResult.level); // 敏感级别（P0/P1/P2/CLEAN）
        result.put("banDays", detectorResult.getBanDays()); // 封禁天数（365/60/0）
        result.put("triggeredKeyword", detectorResult.triggeredKeyword); // 触发的首个敏感词
        // 将所有检测到的敏感词用逗号拼接（原方法用字符串存储，保持格式兼容）
        result.put("foundWords", String.join(",", detectorResult.foundWords));

        return result;
    }

    /**
     * 安全转换对象为Long
     */
    private static Long convertToLong(Object obj) {
        if (obj == null) return null;
        try {
            if (obj instanceof Number) {
                return ((Number) obj).longValue();
            } else if (obj instanceof String) {
                return Long.parseLong((String) obj);
            }
        } catch (Exception e) {
            System.err.println("Long转换失败: " + obj);
        }
        return null;
    }

    /**
     * 安全获取字符串
     */
    private static String getStringSafe(JSONObject json, String key) {
        Object obj = json.get(key);
        return obj != null ? obj.toString() : null;
    }

    /**
     * 解析金额字段（若数据中无 total_amount，返回 null）
     */
    private static Double parseAmountField(JSONObject data) {
        if (data.containsKey("total_amount")) {
            Object amountNode = data.get("total_amount");
            return convertToDouble(amountNode);
        }
        return null;
    }

    /**
     * 安全转换对象为Double
     */
    private static Double convertToDouble(Object obj) {
        if (obj == null) return null;
        try {
            if (obj instanceof Number) {
                return ((Number) obj).doubleValue();
            } else if (obj instanceof String) {
                String amountStr = ((String) obj).trim();
                amountStr = amountStr.replaceAll("[^\\d.]", "");
                if (!amountStr.isEmpty() && amountStr.matches("^\\d+(\\.\\d+)?$")) {
                    return Double.parseDouble(amountStr);
                }
            }
        } catch (Exception e) {
            System.err.println("Double转换失败: " + obj);
        }
        return null;
    }

    /**
     * 从评论中提取金额
     */
    private static Double extractAmountFromComment(String commentContent) {
        if (commentContent == null) return null;

        String[] patterns = {
                "(\\d{1,10}[.,]?\\d{0,2})\\s*(元|块|人民币|RMB|¥)",
                "价格.*?(\\d{1,10}[.,]?\\d{0,2})",
                "花了.*?(\\d{1,10}[.,]?\\d{0,2})",
                "买.*?(\\d{1,10}[.,]?\\d{0,2})",
                "\\b(\\d{3,5})\\b"
        };

        for (String patternStr : patterns) {
            try {
                Pattern pattern = Pattern.compile(patternStr);
                Matcher matcher = pattern.matcher(commentContent);

                if (matcher.find()) {
                    String amountStr = "";
                    if (matcher.groupCount() >= 1) {
                        amountStr = matcher.group(1);
                    } else {
                        amountStr = matcher.group();
                    }

                    amountStr = amountStr.replace(",", "").replace("，", "").replace(" ", "")
                            .replace("元", "").replace("块", "");

                    try {
                        double amount = Double.parseDouble(amountStr);
                        if (amount >= 100 && amount <= 100000) {
                            return amount;
                        }
                    } catch (NumberFormatException e) {
                        // 忽略格式错误
                    }
                }
            } catch (Exception e) {
                System.err.println("正则表达式匹配异常: " + e.getMessage());
            }
        }

        return null;
    }

    /**
     * 直接提取金额
     */
    private static int extractAmountDirectly(String commentContent) {
        if (commentContent == null) return 0;

        Pattern numberPattern = Pattern.compile("\\b(\\d{3,5})\\b");
        Matcher matcher = numberPattern.matcher(commentContent);

        while (matcher.find()) {
            String numberStr = matcher.group(1);
            try {
                int amount = Integer.parseInt(numberStr);
                if (amount >= 100 && amount <= 100000) {
                    return amount;
                }
            } catch (NumberFormatException e) {
                // 忽略
            }
        }

        return 0;
    }
}