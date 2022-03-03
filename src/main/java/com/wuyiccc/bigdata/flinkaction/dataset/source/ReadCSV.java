package com.wuyiccc.bigdata.flinkaction.dataset.source;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @author wuyiccc
 * @date 2022/3/4 7:02
 * 读取和解析CSV文件
 */
public class ReadCSV {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<User> inputData = env.readCsvFile("./file/test.csv")
                .fieldDelimiter(",")
                .ignoreFirstLine()
                .includeFields(true, false, true)
                .pojoType(User.class, "name", "age");
        inputData.print();
    }
}
