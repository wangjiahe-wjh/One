package com;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消费Kafka评论/日志Topic，整合输出user_info_label格式JSON
 */
public class UserInfoLabelConsumer {
    // Kafka配置（替换为你的实际地址）
    private static final String KAFKA_BROKER = "192.168.200.32:9092";
    private static final String CONSUMER_GROUP = "user-info-label-group";
    private static final String[] TOPICS = {"realtime_v3_comment_cdc", "realtime_v3_logs"};

    // 缓存：按user_id存储用户整合数据
    private static final ConcurrentHashMap<String, JSONObject> userCache = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(4); // 异步处理线程池

        // Kafka消费者配置
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPICS));
        System.out.println("开始消费并整合user_info_label...");

        // 异步消费主逻辑
        executor.submit(() -> {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(record -> {
                    executor.submit(() -> processMessage(record.topic(), record.value()));
                });
                consumer.commitAsync();
            }
        });

        // 关闭资源
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("停止消费...");
            consumer.close();
            executor.shutdown();
        }));
    }

    /**
     * 处理单条Kafka消息，整合到user_info_label
     */
    private static void processMessage(String topic, String jsonStr) {
        try {
            JSONObject msg = JSON.parseObject(jsonStr);
            String userId = msg.getString("user_id");
            if (userId == null) return;

            // 初始化用户数据（首次出现该user_id）
            JSONObject userLabel = userCache.computeIfAbsent(userId, k -> initUserLabel(userId));

            // 区分评论/日志Topic，填充对应字段
            if ("realtime_v3_comment_cdc".equals(topic)) {
                fillCommentData(userLabel, msg);
            } else if ("realtime_v3_logs".equals(topic)) {
                fillLogData(userLabel, msg);
            }

            // 输出最终的user_info_label（格式化JSON）
            System.out.println("=== 生成user_info_label ===");
            System.out.println(JSON.toJSONString(userLabel, String.valueOf(true)));
            System.out.println("---------------------------\n");

        } catch (Exception e) {
            System.err.printf("处理消息失败：%s\n", e.getMessage());
        }
    }

    /**
     * 初始化user_info_label的基础结构
     */
    private static JSONObject initUserLabel(String userId) {
        JSONObject userLabel = new JSONObject();
        userLabel.put("userid", userId);
        userLabel.put("username", ""); // 无数据则为空
        userLabel.put("user_base_info", new JSONObject()); // 空对象
        userLabel.put("login_time", new ArrayList<>()); // 空列表
        userLabel.put("consumption_level", "");
        userLabel.put("device_info", new JSONObject());
        userLabel.put("search_info", new JSONObject());
        userLabel.put("category_info", new JSONObject());
        userLabel.put("shoping_gender", new JSONObject()
                .fluentPut("gender", "")
                .fluentPut("shoping_id", new ArrayList<>()));
        userLabel.put("is_check_sensitive_comment", "0"); // 默认非敏感
        userLabel.put("sensitive_word", new ArrayList<>());
        userLabel.put("ds", "");
        userLabel.put("ts", "");
        return userLabel;
    }

    /**
     * 填充评论Topic的数据到user_info_label
     */
    private static void fillCommentData(JSONObject userLabel, JSONObject commentMsg) {
        // 敏感评论标记
        boolean isInsulting = commentMsg.getBooleanValue("is_insulting");
        userLabel.put("is_check_sensitive_comment", isInsulting ? "1" : "0");

        // 敏感词信息（如果有）
        if (isInsulting) {
            List<JSONObject> sensitiveList = userLabel.getList("sensitive_word", JSONObject.class);
            JSONObject sensitive = new JSONObject();
            sensitive.put("trigger_time", commentMsg.getString("created_time"));
            sensitive.put("trigger_word", commentMsg.getString("triggered_keyword"));
            sensitive.put("orderid", commentMsg.getString("order_id"));
            sensitiveList.add(sensitive);
        }

        // 时间字段
        userLabel.put("ds", commentMsg.getString("ds"));
        userLabel.put("ts", commentMsg.getString("ts"));
    }

    /**
     * 填充日志Topic的数据到user_info_label
     */
    private static void fillLogData(JSONObject userLabel, JSONObject logMsg) {
        // 设备信息
        userLabel.put("device_info", logMsg.getJSONObject("device"));

        // 登录时间（添加当前日志时间）
        List<String> loginTimeList = userLabel.getList("login_time", String.class);
        loginTimeList.add(new Date(logMsg.getLongValue("ts") * 1000).toString()); // 时间戳转字符串

        // 时间字段
        userLabel.put("ts", logMsg.getString("ts"));
    }
}