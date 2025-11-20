package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDataQuery {
    public static void main(String[] args) {
        // 1. 配置HBase连接（ZooKeeper地址和集群一致）
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03"); // 你的ZooKeeper节点
        conf.set("hbase.zookeeper.property.clientPort", "2181"); // 默认端口

        try (
                // 2. 创建连接和表对象
                Connection connection = ConnectionFactory.createConnection(conf);
                Table table = connection.getTable(TableName.valueOf("default", "user_info_base"))
        ) {
            // 3. 构建查询条件（查询前10条）
            Scan scan = new Scan();
            scan.setLimit(10); // 限制返回10条

            // 4. 执行查询并遍历结果
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                // 解析RowKey
                String rowKey = Bytes.toString(result.getRow());
                // 关键修正：将 info:name 改为 info:uname（对应PG表的 uname 字段）
                String uname = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("uname")));
                String age = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));

                // 打印数据（显示uname）
                System.out.printf("RowKey: %s, uname: %s, age: %s%n", rowKey, uname, age);
            }
            scanner.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}