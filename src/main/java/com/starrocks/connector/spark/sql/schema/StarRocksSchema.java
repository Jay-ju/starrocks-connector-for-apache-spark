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

package com.starrocks.connector.spark.sql.schema;

import com.starrocks.connector.spark.sql.preprocessor.EtlJobConfig;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.connector.spark.sql.schema.StarRocksField.__OP;

public class StarRocksSchema implements Serializable {
    private List<StarRocksField> columns;
    private List<StarRocksField> pks;
    private Map<String, StarRocksField> columnMap;

    private EtlJobConfig.EtlTable etlTable;
    private Long tableId;

    // only use for schema
    public StarRocksSchema(List<StarRocksField> columns, List<StarRocksField> pks) {
        this.columns = columns;
        this.pks = pks;
    }

    public StarRocksSchema(List<StarRocksField> columns, List<StarRocksField> pks,
                           EtlJobConfig.EtlTable etlTable, Long tableId) {
        this.columns = columns;
        this.pks = pks;
        this.columnMap = new HashMap<>();
        for (StarRocksField field : columns) {
            columnMap.put(field.getName(), field);
        }
        this.etlTable = etlTable;
        this.tableId = tableId;
    }

    public List<StarRocksField> getColumns() {
        return columns;
    }

    public boolean isPrimaryKey() {
        return !pks.isEmpty();
    }

    public StarRocksField getField(String columnName) {
        if (__OP.getName().equalsIgnoreCase(columnName)) {
            return __OP;
        }

        return columnMap.get(columnName);
    }

    public EtlJobConfig.EtlTable getEtlTable() {
        return etlTable;
    }

    public Long getTableId() {
        return tableId;
    }

}
