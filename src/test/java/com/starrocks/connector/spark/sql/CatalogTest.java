// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.spark.sql;

import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CatalogTest extends ITTestBase {

    private static final String simpleTableName = "testSql_1";
    private static final String allTypeTableName = "all_type_tb";
    private static final String allTypeDstTableName = "all_type_tb_new";
    private static final String aggTableName = "agg_table";

    String simpleTableDdl = String.format("CREATE TABLE IF NOT EXISTS `%s`.`%s` (" +
                            "id INT," +
                            "name STRING," +
                            "score INT" +
                            ") ENGINE=OLAP " +
                            "PRIMARY KEY(`id`) " +
                            "DISTRIBUTED BY HASH(`id`) BUCKETS 2 " +
                            "PROPERTIES (" +
                            "\"replication_num\" = \"1\"" +
                            ")",
                    DB_NAME, simpleTableName);
    String allTypeTableDdl = String.format("CREATE TABLE IF NOT EXISTS `%s`.`%s`(\n" +
                            "    tinyint_             tinyint         NOT NULL    COMMENT \"tinyint_\",\n" +
                            "    smallint_            smallint        NOT NULL    COMMENT \"smallint_\",\n" +
                            "    int_                 int             NOT NULL    COMMENT \"int_\",\n" +
                            "    bigint_              bigint          NOT NULL    COMMENT \"bigint_\",\n" +
                            "    date_                date          NOT NULL    COMMENT \"date_\",\n" +
                            // "    largeint_            largeint        NOT NULL    COMMENT \"largeint_\",\n" +
                            "    decimal_             decimal(10, 4)         NOT NULL    COMMENT \"decimal_\",\n" +
                            "    double_              double          NOT NULL    COMMENT \"double_\",\n" +
                            "    float_               float           NOT NULL    COMMENT \"float_\",\n" +
                            "    boolean_             boolean         NOT NULL    COMMENT \"boolean_\",\n" +
                            "    tinyint_value        tinyint         NOT NULL    COMMENT \"tinyint_\",\n" +
                            "    smallint_value       smallint        NOT NULL    COMMENT \"smallint_\",\n" +
                            "    int_value            int             NOT NULL    COMMENT \"int_\",\n" +
                            "    date_value           date             NOT NULL    COMMENT \"date_\",\n" +
                            "    bigint_value         bigint          NOT NULL    COMMENT \"bigint_\"\n" +
                            // large int spark can't support
                            // "    largeint_value       largeint        NOT NULL    COMMENT \"largeint_\"\n" +
                            ") \n" +
                            "duplicate key(tinyint_, smallint_, int_, bigint_)\n" +
                            "distributed by hash(tinyint_, smallint_, int_, bigint_) buckets 4\n" +
                            "properties(\n" +
                            "    \"replication_num\" = \"1\"\n" +
                            ")\n" +
                            ";",
                    DB_NAME, allTypeTableName);

    String aggTableDdl = String.format("CREATE TABLE IF NOT EXISTS %s.%s (\n" +
            "    site_id LARGEINT NOT NULL COMMENT \"id of site\",\n" +
            "    date DATE NOT NULL COMMENT \"time of event\",\n" +
            "    city_code VARCHAR(20) COMMENT \"city_code of user\",\n" +
            "    pv BIGINT SUM DEFAULT \"0\" COMMENT \"total page views\"\n" +
            ")\n" +
            "AGGREGATE KEY(site_id, date, city_code)\n" +
            "DISTRIBUTED BY HASH(site_id)\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"3\"\n" +
            ");", DB_NAME, aggTableName);

    private static final String FS_S3A_ENDPOINT= "https://tos-s3-cn-beijing.ivolces.com";
    private static final String FS_S3A_ENDPOINT_REGION= "cn-beijing";
    private static final String FS_S3A_CONNECTION_SSL_ENABLED= "true";
    private static final String FS_S3A_PATH_STYLE_ACCESS= "false";
    private static final String FS_S3A_ACCESS_KEY= "AKLTZDhlZWFiYWRiMzY0NGEyYjljMzNiNjU0Njc4OGJmOGY";
    private static final String FS_S3A_SECRET_KEY= "T1RsbU9HUmxOREJtTjJRMU5EaG1ZV0UwWlRaa1ltVmhNVGszT1dVeU5HRQ==";

    private SparkSession spark;

    @Before
    public void prepare() throws Exception {
        String createStarRocksDB = String.format("CREATE DATABASE IF NOT EXISTS %s", DB_NAME);
        executeSrSQL(createStarRocksDB);
        executeSrSQL(simpleTableDdl);
        executeSrSQL(allTypeTableDdl);
        executeSrSQL(aggTableDdl);

        spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testLocalSql")
                .config("spark.sql.catalog.starrocks", "com.starrocks.connector.spark.catalog.StarRocksCatalog")
                .config("spark.sql.extensions", "com.starrocks.connector.spark.StarRocksExtensions")
                .config("spark.sql.codegen.wholeStage", "false")
                .config("spark.sql.codegen.factoryMode", "NO_CODEGEN")
                .config("spark.sql.defaultCatalog", "starrocks")
                .config("spark.sql.catalog.starrocks.fe.http.url", FE_HTTP)
                .config("spark.sql.catalog.starrocks.fe.jdbc.url", FE_JDBC)
                .config("spark.sql.catalog.starrocks.password", PASSWORD)
                .config("spark.sql.catalog.starrocks.user", USER)
                .config("spark.sql.catalog.starrocks.fs.s3a.endpoint", FS_S3A_ENDPOINT)
                .config("spark.sql.catalog.starrocks.fs.s3a.endpoint.region", FS_S3A_ENDPOINT_REGION)
                .config("spark.sql.catalog.starrocks.fs.s3a.connection.ssl.enabled", FS_S3A_CONNECTION_SSL_ENABLED)
                .config("spark.sql.catalog.starrocks.fs.s3a.path.style.access", FS_S3A_PATH_STYLE_ACCESS)
                .config("spark.sql.catalog.starrocks.fs.s3a.access.key", FS_S3A_ACCESS_KEY)
                .config("spark.sql.catalog.starrocks.fs.s3a.secret.key", FS_S3A_SECRET_KEY)
                .getOrCreate();
    }

    @After
    public void close() throws Exception {
        List<String> tableNames = ImmutableList.of(simpleTableName, allTypeTableName, aggTableName);
        for (String tableName : tableNames) {
            String dropTable = String.format("DROP TABLE IF EXISTS `%s`.`%s` ", DB_NAME, tableName);
            executeSrSQL(dropTable);
        }

        String dropDB = String.format("DROP Database IF EXISTS `%s` ", DB_NAME);
        executeSrSQL(dropDB);
        spark.stop();
    }

    @Test
    public void testCatalog() {
        String listDb = "show databases";
        spark.sql(listDb).show();
        String changeDb = String.format("use starrocks.%s", DB_NAME);
        spark.sql(changeDb).show();
        String listTables = "show tables";
        spark.sql(listTables).show();

    }

    @Test
    public void testSimpleTable() throws SQLException {
        // simple table
        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList(22, "null", "null"));
        expectedData.add(Arrays.asList(2, "3", 4));
        expectedData.add(Arrays.asList(1, "3", 4));

        String insertSql = String.format("INSERT INTO %s.%s VALUES (22, null, null), (2, '3', 4), (1, '3', 4)", DB_NAME, simpleTableName);
        spark.sql("explain " + insertSql).show(100000, false);
        spark.sql(insertSql).show();
        String selectSql = String.format("select * from %s.%s", DB_NAME, simpleTableName);
        spark.sql(selectSql).show();

        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, simpleTableName);
        verifyResult(expectedData, actualWriteData);
    }

    @Test
    public void testAllTypeSql() throws Exception {
        List<List<Object>> expectedData = new ArrayList<>();
        // all type
        expectedData.add(Arrays.asList(127, 32767, 2147483647, 9223372036854775807L, "2023-02-01",
                12.35, 3.1415926, 3.14, false, 127, 32767, 2147483647, 9223372036854775807L));
        String insertSql = String.format("insert into %s.%s values(127, 32767, 2147483647, 9223372036854775807, '2023-02-01'," +
                        " 12.35, 3.1415926, 3.14, false, 127, 32767, 2147483647, 9223372036854775807)",
                DB_NAME, allTypeTableName);
        spark.sql(insertSql).show();
        String selectSql = String.format("SELECT * FROM %s.%s", DB_NAME, allTypeTableName);
        List<Row> readRows = spark.sql(selectSql).collectAsList();
        verifyRows(expectedData, readRows);

        spark.stop();
    }

    @Test
    public void testAggTable() throws Exception {
        // simple table
        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList(2, "3", 4));
        expectedData.add(Arrays.asList(1, "3", 4));

        String insertSql = String.format("INSERT INTO %s.%s VALUES (22, null, null), (2, '3', 4), (1, '3', 4)", DB_NAME, simpleTableName);
        spark.sql("explain " + insertSql).show(100000, false);
        spark.sql(insertSql).show();
        String selectSql = String.format("select * from %s.%s", DB_NAME, simpleTableName);
        spark.sql(selectSql).show();

        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, simpleTableName);
        verifyResult(expectedData, actualWriteData);

        // all type
        expectedData.clear();
        expectedData.add(Arrays.asList(127, 32767, 2147483647, 9223372036854775807L,
                12.35, 3.1415926, 3.14, false, 127, 32767, 2147483647, 9223372036854775807L));
        insertSql = String.format("insert into %s.%s values(127, 32767, 2147483647, 9223372036854775807," +
                        " 12.35, 3.1415926, 3.14, false, 127, 32767, 2147483647, 9223372036854775807)",
                DB_NAME, allTypeTableName);
        spark.sql(insertSql).show();
        selectSql = String.format("SELECT * FROM %s.%s", DB_NAME, allTypeTableName);
        List<Row> readRows = spark.sql(selectSql).collectAsList();
        verifyRows(expectedData, readRows);

        spark.stop();
    }

}