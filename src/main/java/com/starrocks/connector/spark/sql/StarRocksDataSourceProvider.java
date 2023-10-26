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

import com.starrocks.connector.spark.cfg.PropertiesSettings;
import com.starrocks.connector.spark.dpp.StreamDppLoadSink;
import com.starrocks.connector.spark.rest.models.Schema;
import com.starrocks.connector.spark.sql.conf.SimpleStarRocksConfig;
import com.starrocks.connector.spark.sql.conf.StarRocksConfig;
import com.starrocks.connector.spark.sql.schema.StarRocksField;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.StreamSinkProvider;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TABLE_IDENTIFIER;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.makeWriteCompatibleWithRead;
import static com.starrocks.connector.spark.sql.StarRocksTable.identifierFullName;
import static com.starrocks.connector.spark.sql.conf.StarRocksConfig.PREFIX;

public class StarRocksDataSourceProvider implements RelationProvider,
        DataSourceRegister,
        StreamSinkProvider,
        CreatableRelationProvider {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksDataSourceProvider.class);

    @Nullable
    private StarRocksSchema starrocksSchema;

    @Override
    public BaseRelation createRelation(SQLContext sqlContext,
                                       scala.collection.immutable.Map<String, String> parameters) {
        Map<String, String> mutableParams = new HashMap<>();
        scala.collection.immutable.Map<String, String> transParameters = Utils.params(parameters, LOG);
        transParameters.toStream().foreach(key -> mutableParams.put(key._1, key._2));
        SimpleStarRocksConfig config = new SimpleStarRocksConfig(makeWriteCompatibleWithRead(mutableParams));
        starrocksSchema = getStarRocksSchema(config);

        return new StarrocksRelation(sqlContext, transParameters);
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode,
                                       scala.collection.immutable.Map<String, String> parameters, Dataset<Row> data) {
        if (starrocksSchema == null) {
            Map<String, String> mutableParams = new HashMap<>();
            parameters.toStream().foreach(key -> mutableParams.put(key._1, key._2));
            SimpleStarRocksConfig config = new SimpleStarRocksConfig(makeWriteCompatibleWithRead(mutableParams));
            starrocksSchema = getStarRocksSchema(config);
        }
        StreamDppLoadSink writer = new StreamDppLoadSink(sqlContext, starrocksSchema);
        writer.write(data);

        return new BaseRelation() {
            @Override
            public SQLContext sqlContext() {
                return null;
            }

            @Override
            public StructType schema() {
                return null;
            }
        };
    }

    @Override
    public String shortName() {
        return "starrocks";
    }

    //TODO 将所有转换的逻辑统一
    private static StarRocksSchema convert(Schema schema) {
        List<StarRocksField> columns = new ArrayList<>();
        List<StarRocksField> pks = new ArrayList<>();

        schema.getProperties().forEach(field -> {
            StarRocksField starRocksField = new StarRocksField(field.getName(), field.getType(), field.getPrecision(),
                    String.valueOf(field.getPrecision()), String.valueOf(field.getPrecision()));
            columns.add(starRocksField);
            if (field.getIsKey()) {
                pks.add(starRocksField);
            }
        });

        return new StarRocksSchema(columns, pks, schema.getEtlTable(), schema.getTableId());
    }

    public static StarRocksSchema getStarRocksSchema(StarRocksConfig config) {
       return getStarRocksSchema(config, null);
    }

    public static StarRocksSchema getStarRocksSchema(StarRocksConfig config, Identifier tbIdentifier) {
        PropertiesSettings properties = new PropertiesSettings();

        if (StringUtils.isEmpty(config.getDatabase())) {
            if (null == tbIdentifier || tbIdentifier.namespace().length == 0) {
                LOG.error("request params must contain table identifier");
                return null;
            } else {
                properties.setProperty(STARROCKS_TABLE_IDENTIFIER, identifierFullName(tbIdentifier));
            }
        }
        Map<String, String> options = makeWriteCompatibleWithRead(config.getOriginOptions());
        options.forEach((k, v) -> properties.setProperty(k, v));
        Schema schema = SchemaUtils.discoverSchemaFromFe(properties);
        if (schema != null) {
            return convert(schema);
        }

        return null;
    }

    public static Map<String, String> addPrefixInStarRockConfig(Map<String, String> options) {
        Map<String, String> properties = new HashMap<>();
        options.forEach((k, v) -> {
            if (k.startsWith(PREFIX)) {
                properties.put(k, v);
            } else {
                properties.put(PREFIX + k, v);
            }
        });
        return properties;
    }

    @Override
    public Sink createSink(SQLContext sqlContext, scala.collection.immutable.Map<String, String> parameters,
                           Seq<String> partitionColumns, OutputMode outputMode) {
        return new StreamDppLoadSink(sqlContext, starrocksSchema);
    }


}