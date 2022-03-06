package com.wuyiccc.bigdata.flinkaction.table.catalog;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;

import java.util.HashMap;
import java.util.List;

import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;
import static org.apache.flink.table.descriptors.GenericInMemoryCatalogValidator.CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY;

/**
 * @author wuyiccc
 * @date 2022/3/6 9:30
 * 使用java方式创建Catalog, Catalog数据库与Catalog表
 */
public class CatalogDemo {

    public static void main(String[] args) throws DatabaseAlreadyExistException, TableAlreadyExistException, DatabaseNotExistException {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 创建Table API 和 SQL执行环境
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        //  创建一个内存Catalog
        Catalog catalog = new GenericInMemoryCatalog(GenericInMemoryCatalog.DEFAULT_DB);

        // 注册Catalog
        tableEnv.registerCatalog("myCatalog", catalog);

        // 创建一个Catalog数据库
        HashMap<String, String> hashMap = new HashMap<>();
        hashMap.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY);
        hashMap.put(CATALOG_PROPERTY_VERSION, "1");
        catalog.createDatabase("myDb", new CatalogDatabaseImpl(hashMap, "comment"), false);
        // 创建一个Catalog表
        TableSchema schema = TableSchema.builder()
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .build();

        catalog.createTable(
                new ObjectPath("myDb", "mytable"),
                new CatalogTableImpl(schema, hashMap, CATALOG_PROPERTY_VERSION),
                false
        );
        catalog.createTable(
                new ObjectPath("myDb", "mytable2"),
                new CatalogTableImpl(schema, hashMap, CATALOG_PROPERTY_VERSION),
                false
        );

        List<String> tables = catalog.listTables("myDb");
        System.out.println(tables.toString());

    }
}
