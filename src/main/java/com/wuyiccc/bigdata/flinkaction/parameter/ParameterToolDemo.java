package com.wuyiccc.bigdata.flinkaction.parameter;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wuyiccc
 * @date 2022/3/2 22:46
 */
public class ParameterToolDemo {


    public static void main(String[] args) throws IOException {
        // 1. 从Map中读取参数
        Map<String, String> properties = new HashMap<>();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("zookeeper.connect", "127.0.0.1:2181");
        properties.put("topic", "myTopic");

        ParameterTool parameterTool = ParameterTool.fromMap(properties);
        System.out.println(parameterTool.getRequired("topic"));
        System.out.println(parameterTool.getProperties());

        // 2. 从properties配置文件中读取参数
        String propertiesPath = "src/main/resources/myjob.properties";
        ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesPath);
        System.out.println(parameter.getProperties());
        System.out.println(parameter.getRequired("my"));

        File propertiesFile = new File(propertiesPath);
        ParameterTool parameterFile = ParameterTool.fromPropertiesFile(propertiesFile);
        System.out.println(parameterFile.getProperties());
        System.out.println(parameterFile.getRequired("my"));

        // 3. 从命令行中读取参数: program args
        ParameterTool parameterFromArgs = ParameterTool.fromArgs(args);
        System.out.println("parameterFromArgs: " + parameterFromArgs.getProperties());

        // 4. 从系统配置中读取参数: vm options
        ParameterTool parameterFromSystemProperties = ParameterTool.fromSystemProperties();
        System.out.println("parameterFromSystemProperties" + parameterFromSystemProperties.getProperties());

    }
}
