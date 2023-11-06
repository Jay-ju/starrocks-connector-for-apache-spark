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

package com.starrocks.connector.spark.sql.write;

import com.starrocks.connector.spark.cfg.SparkSettings;
import com.starrocks.connector.spark.rest.RestService;
import com.starrocks.connector.spark.rest.models.TabletCommitInfo;
import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.preprocessor.EtlJobConfig;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import com.starrocks.format.StarrocksWriter;
import com.starrocks.proto.TabletSchema;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.starrocks.connector.spark.rest.RestService.TXN_COMMIT;
import static com.starrocks.connector.spark.rest.RestService.TXN_PREPARE;
import static com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig.FOR_TEST_RAW_WRITER;

public class StarRocksWrite implements BatchWrite, StreamingWrite {

    private static final Logger log = LoggerFactory.getLogger(StarRocksWrite.class);

    private final LogicalWriteInfo logicalInfo;
    private final WriteStarRocksConfig config;
    private final StarRocksSchema schema;

    // for sr
    private TransactionContext transactionContext;
    private SparkSettings sparkSettings;


    public StarRocksWrite(LogicalWriteInfo logicalInfo,
                          WriteStarRocksConfig config,
                          StarRocksSchema schema) {
        this.logicalInfo = logicalInfo;
        this.config = config;
        this.schema = schema;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        // begin transaction
        if (FOR_TEST_RAW_WRITER) {
            sparkSettings = new SparkSettings(SparkContext.getOrCreate().getConf());
            sparkSettings.setProperty("db", config.getDatabase());
            sparkSettings.setProperty("table", config.getTable());
            transactionContext = RestService.beginTransation(sparkSettings);

            sparkSettings.setProperty("txn_id", String.valueOf(transactionContext.getTxnId()));
            sparkSettings.setProperty("label", transactionContext.getLabel());
        }

        return new StarRocksWriterFactory(logicalInfo.schema(), config, schema,
                transactionContext == null ? -1 :transactionContext.getTxnId());
    }

    @Override
    public boolean useCommitCoordinator() {
        return true;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        if (FOR_TEST_RAW_WRITER) {
            List<TabletCommitInfo> tabletCommitInfos = Arrays.asList(messages)
                    .stream()
                    .filter(message -> ((StarRocksWriterCommitMessage) message).getTabletCommitInfo() != null)
                    .map(message -> ((StarRocksWriterCommitMessage) message).getTabletCommitInfo())
                    .collect(Collectors.toList());
            List<Long> writeTabletIds = tabletCommitInfos.stream()
                    .map(TabletCommitInfo::getTabletId)
                    .collect(Collectors.toList());

            List<EtlJobConfig.EtlPartition> partitions = schema.getEtlTable().getPartitionInfo().getPartitions();
            // TODO support multi partition
            String rootPath = partitions.get(0).getStoragePath();
            List<Long> allTabletIds = partitions.get(0).getTabletIds();
            List<Long> allBackendIds = partitions.get(0).getBackendIds();

            Map<Long, Long> tablet2Backend = IntStream.range(0, allTabletIds.size())
                    .boxed()
                    .collect(Collectors.toMap(allTabletIds::get, allBackendIds::get));

            // only have unwritten tablets
            allTabletIds.removeAll(writeTabletIds);

            TabletSchema.TabletSchemaPB pbSchema = schema.getEtlTable().convert();
            Long txnId = ((StarRocksWriterCommitMessage) messages[0]).getTxnId();

            allTabletIds.forEach(unwrittenTabletId -> {
                Map<String, String> configMap = config.getOriginOptions();
                configMap.put("writer_type", "0");
                StarrocksWriter starrocksWriter =
                        new StarrocksWriter(unwrittenTabletId, pbSchema, txnId, rootPath, configMap);
                // will write txn log
                starrocksWriter.open();
                starrocksWriter.finish();
                starrocksWriter.close();
                starrocksWriter.release();
            });

            RestService.preOrCommitTransaction(sparkSettings, tabletCommitInfos, TXN_PREPARE);
            RestService.preOrCommitTransaction(sparkSettings, tabletCommitInfos, TXN_COMMIT);
        }

        log.info("batch query `{}` commit", logicalInfo.queryId());
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        log.info("batch query `{}` abort", logicalInfo.queryId());
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
        // not implemented
        return new StarRocksWriterFactory(logicalInfo.schema(), config, schema, transactionContext.getTxnId());
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
        log.info("streaming query `{}` commit", logicalInfo.queryId());
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        log.info("streaming query `{}` abort", logicalInfo.queryId());
    }

    public LogicalWriteInfo getLogicalInfo() {
        return logicalInfo;
    }

    public WriteStarRocksConfig getConfig() {
        return config;
    }

    public StarRocksSchema getSchema() {
        return schema;
    }

}
