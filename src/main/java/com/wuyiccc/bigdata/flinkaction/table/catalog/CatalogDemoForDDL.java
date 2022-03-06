package com.wuyiccc.bigdata.flinkaction.table.catalog;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;
import static org.apache.flink.table.descriptors.GenericInMemoryCatalogValidator.CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY;

public class CatalogDemoForDDL {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        HashMap<String, String> hashMap = new HashMap<String, String>();
        hashMap.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY);
        hashMap.put(CATALOG_PROPERTY_VERSION, "1");
        // 创建一个内存Catalog
        Catalog catalog = new GenericInMemoryCatalog(GenericInMemoryCatalog.DEFAULT_DB);

        // 注册Catalog
        tableEnv.registerCatalog("mycatalog", catalog);


        // 创建一个Catalog数据库
        tableEnv.executeSql("CREATE DATABASE mydb ");

        //创建一个Catalog表
        tableEnv.executeSql("CREATE TABLE mytable (name STRING, age INT) ");
        tableEnv.executeSql("CREATE TABLE mytable2 (name STRING, age INT) ");

        List<String> tables = Arrays.asList(tableEnv.listTables().clone()); //
        System.out.println("表信息：" + tables.toString());

    }
}
