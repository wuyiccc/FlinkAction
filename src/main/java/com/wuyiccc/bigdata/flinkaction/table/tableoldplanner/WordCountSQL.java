package com.wuyiccc.bigdata.flinkaction.table.tableoldplanner;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
public class WordCountSQL {
	public static void main(String[] args) throws Exception {

		//获取执行环境
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

		DataSet<WC> input = env.fromElements(
				new WC("Hello", 1),
				new WC("Flink", 1),
				new WC("Hello", 1));

		//注册DataSet为view："WordCount"
		tEnv.createTemporaryView("WordCount", input, $("word"), $("frequency"));

		//在注册的表上执行SQL查询并把取回的结果作为一个新的Table
		Table table = tEnv.sqlQuery(
			"SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");

		DataSet<WC> result = tEnv.toDataSet(table, WC.class);

		result.print();
	}

}
