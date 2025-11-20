package com;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.HbaseUtils;

import java.sql.*;
import java.util.ArrayList;

public class PgToHBaseSync {
    // ====================== 你需要修改的参数（8个）======================
    // 1. PG数据库连接信息
    private static final String PG_URL = "jdbc:postgresql://192.168.200.32:5432/spider_db"; // 例：jdbc:postgresql://192.168.200.32:5432/spider_db
    private static final String PG_USER = "postgres"; // 例：postgres
    private static final String PG_PASSWORD = "123456"; // 例：123456
    private static final String PG_TABLE = "user_info_base"; // 要同步的PG表名（不用改）

    // 2. HBase配置
    private static final String HBASE_ZK = "cdh01,cdh02,cdh03"; // ZooKeeper地址（不用改，和你HbaseUtils一致）
    private static final String HBASE_NAMESPACE = "default"; // HBase命名空间（比如你之前用的ns_zxn，没有就填default）
    private static final String HBASE_TABLE = "user_info_base"; // HBase目标表名（和PG表名一致，好记）
    private static final String PG_PRIMARY_KEY = "user_id"; // PG表的主键（作为HBase的行键RowKey，必须唯一！比如user_id）
    // ==================================================================

    // 批量写入大小（1000条一批，不用改）
    private static final int BATCH_SIZE = 1000;
    private static final Logger LOG = LoggerFactory.getLogger(PgToHBaseSync.class);

    public static void main(String[] args) {
        HbaseUtils hbaseUtils = null;
        Connection pgConn = null;
        BufferedMutator hbaseWriter = null;

        try {
            // 1. 连接HBase，自动创建目标表（列族是info，和你HbaseUtils默认一致）
            hbaseUtils = new HbaseUtils(HBASE_ZK);
            hbaseUtils.createTable(HBASE_NAMESPACE, HBASE_TABLE, "info"); // 建表：命名空间+表名+列族info
            LOG.info("HBase表创建成功（已存在则跳过）：{}:{}", HBASE_NAMESPACE, HBASE_TABLE);

            // 2. 初始化HBase批量写入器（提高效率，减少卡顿）
            BufferedMutatorParams writerParams = new BufferedMutatorParams(TableName.valueOf(HBASE_NAMESPACE, HBASE_TABLE));
            writerParams.writeBufferSize(1024 * 1024 * 5); // 5MB缓冲区（不用改）
            hbaseWriter = hbaseUtils.getConnection().getBufferedMutator(writerParams);

            // 3. 连接PG数据库，读取数据
            pgConn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASSWORD);
            Statement stmt = pgConn.createStatement();
            ResultSet pgData = stmt.executeQuery("SELECT * FROM " + PG_TABLE); // 查询PG表所有数据
            ResultSetMetaData pgColumns = pgData.getMetaData(); // 获取PG表的字段名（比如user_id、birthday等）
            int columnCount = pgColumns.getColumnCount(); // PG表总字段数

            // 4. 循环读取PG数据，批量写入HBase
            int totalCount = 0; // 统计同步总数
            ArrayList<JSONObject> batchData = new ArrayList<>(BATCH_SIZE);

            while (pgData.next()) {
                // 4.1 构建一行数据（JSON格式：key=字段名，value=字段值）
                JSONObject rowData = new JSONObject();
                // 4.2 获取HBase的行键RowKey（用PG的主键，比如user_id，确保唯一）
                String rowKey = pgData.getString(PG_PRIMARY_KEY);

                // 4.3 遍历PG表所有字段，存入JSON（包括constellation、age、generation）
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = pgColumns.getColumnName(i); // 字段名（比如birthday）
                    Object columnValue = pgData.getObject(i); // 字段值（比如1994-11-14）
                    // 空值处理：PG字段为空就存空字符串，避免HBase存NULL
                    rowData.put(columnName, columnValue != null ? columnValue.toString() : "");
                }

                // 4.4 调用HbaseUtils的put方法，添加到批量写入器
                HbaseUtils.put(rowKey, rowData, hbaseWriter);
                totalCount++;

                // 4.5 每满1000条提交一次（避免内存溢出）
                if (totalCount % BATCH_SIZE == 0) {
                    hbaseWriter.flush(); // 提交批次
                    LOG.info("已同步 {} 条数据到HBase", totalCount);
                }
            }

            // 5. 提交最后一批剩余数据
            hbaseWriter.flush();
            LOG.info("同步完成！共同步 {} 条数据到HBase表：{}:{}", totalCount, HBASE_NAMESPACE, HBASE_TABLE);

        } catch (Exception e) {
            LOG.error("同步失败！原因：", e);
            System.err.println("同步出错了：" + e.getMessage());
        } finally {
            // 6. 关闭连接（避免资源泄露，必须加）
            try {
                if (hbaseWriter != null) hbaseWriter.close();
                if (pgConn != null) pgConn.close();
                if (hbaseUtils != null && hbaseUtils.getConnection() != null) {
                    hbaseUtils.getConnection().close();
                }
            } catch (Exception e) {
                LOG.error("关闭连接失败：", e);
            }
        }
    }
}