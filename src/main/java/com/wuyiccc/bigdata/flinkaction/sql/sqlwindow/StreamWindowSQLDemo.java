package com.wuyiccc.bigdata.flinkaction.sql.sqlwindow;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;


public class StreamWindowSQLDemo {

	public static void main(String[] args) throws Exception {
		// 设置执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		// 将源数据写入临时文件
		String contents =
			"1,A,3,2020-11-11 00:00:01\n" +
			"2,B,4,2020-11-11 00:00:02\n" +
			"4,C,3,2020-11-11 00:00:04\n" +
			"6,D,3,2020-11-11 00:00:06\n" +
			"34,A,2,2020-11-11 00:00:34\n" +
			"26,A,2,2020-11-11 00:00:26\n" +
			"8,B,1,2020-11-11 00:00:08";
		// 获取绝对路径
		String path = createTempFile(contents);

		//使用DDL注册带有水印的表。事件混乱，因此，我们需要3秒钟的时间来等待较晚的事件
		String ddl = "CREATE TABLE Orders (\n" +
			"  order_id INT,\n" +
			"  product STRING,\n" +
			"  amount INT,\n" +
			"  ts TIMESTAMP(3),\n" +
			"  WATERMARK FOR ts AS ts - INTERVAL '3' SECOND\n" +
			") WITH (\n" +
			"  'connector.type' = 'filesystem',\n" +
			"  'connector.path' = '" + path + "',\n" +
			"  'format.type' = 'csv'\n" +
			")";
		tEnv.executeSql(ddl).print();
		// 打印Schema
		tEnv.executeSql("DESCRIBE Orders").print();

		// 在表上运行SQL查询，并将检索结果作为新表
		String query = "SELECT\n" +
			"  CAST(TUMBLE_START(ts, INTERVAL '5' SECOND) AS STRING) window_start,\n" +
			"  COUNT(*) order_num,\n" +
			"  SUM(amount) amount_num,\n" +
			"  COUNT(DISTINCT product) products_num\n" +
			"FROM Orders\n" +
			"GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";
		Table result = tEnv.sqlQuery(query);
		result.printSchema();
		tEnv.toAppendStream(result, Row.class).print();


		//将表格程序转换为DataStream程序后，必须使用`env.execute()`提交作业。
		env.execute("SQL Job");

	}

	/**
	 * 用contents创建一个临时文件并返回绝对路径。
	 */
	private static String createTempFile(String contents) throws IOException {
		File tempFile = File.createTempFile("Orders", ".csv");
		tempFile.deleteOnExit();
		FileUtils.writeFileUtf8(tempFile, contents);
		return tempFile.toURI().toString();
	}
}
