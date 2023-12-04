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

import com.starrocks.connector.spark.rest.models.TabletCommitInfo;
import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.preprocessor.EtlJobConfig;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import com.starrocks.connector.spark.util.EnvUtils;
import com.starrocks.format.Chunk;
import com.starrocks.format.Column;
import com.starrocks.format.StarrocksWriter;
import com.starrocks.proto.TabletSchema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.removePrefix;

public class StarRocksRawWriter extends StarRocksWriter {
    private static final Logger log = LoggerFactory.getLogger(StarRocksRawWriter.class);

    private final WriteStarRocksConfig config;
    private final StarRocksSchema schema;
    private final int partitionId;
    private final long taskId;
    private final long epochId;
    private final long txnId;

    private TabletSchema.TabletSchemaPB pbSchema;
    private StarrocksWriter starrocksWriter;
    private Chunk chunk;
    private long tabletId;
    private long backendId;
    private boolean isFirstRecord = true;

    public StarRocksRawWriter(WriteStarRocksConfig config,
                              StarRocksSchema schema,
                              int partitionId,
                              long taskId,
                              long epochId,
                              long txnId) {
        this.config = config;
        this.schema = schema;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        this.pbSchema = schema.getEtlTable().convert();
        this.txnId = txnId;
    }

    @Override
    public void open() {
        log.info("Open data writer for partition: {}, task: {}, epoch: {}, {}",
                partitionId, taskId, epochId, EnvUtils.getGitInformation());
    }

    @Override
    public void write(InternalRow internalRow) throws IOException {
        if (isFirstRecord) {
            newStarRocksWriter(internalRow);
        }
        // TODO only write 1 row
        chunk = starrocksWriter.newChunk(1);
        internalRow2Chunk(internalRow);
        starrocksWriter.write(chunk);
        System.out.println("=== debug ===" + chunk.debugRow(0) + ", tablet id" + tabletId);
        chunk.release();
        log.info("partitionId: {}, taskId: {}, epochId: {}, receive raw row: {}",
                partitionId, taskId, epochId, internalRow);
    }

    @Override
    public WriterCommitMessage commit() {
        log.info("partitionId: {}, taskId: {}, epochId: {} commit", partitionId, taskId, epochId);
        if (starrocksWriter == null) {
            return new StarRocksWriterCommitMessage(partitionId, taskId, epochId, null, null, txnId);
        } else {
            starrocksWriter.flush();
            starrocksWriter.finish();
        }
        return new StarRocksWriterCommitMessage(partitionId, taskId, epochId, null, new TabletCommitInfo(tabletId, backendId), txnId);
    }

    @Override
    public void abort() throws IOException {
        log.info("partitionId: {}, taskId: {}, epochId: {} abort", partitionId, taskId, epochId);
    }

    @Override
    public void close() throws IOException {
        log.info("partitionId: {}, taskId: {}, epochId: {} close", partitionId, taskId, epochId);
        if (null != starrocksWriter) {
            starrocksWriter.close();
            starrocksWriter.release();
        }
    }

    private void newStarRocksWriter(InternalRow internalRow) {
        int shemaSize = schema.getColumns().size();
        String partitionInfo = internalRow.getString(shemaSize);
        String[] split = partitionInfo.split("\u0001");
        tabletId = Long.parseLong(split[0]);
        backendId = Long.valueOf(split[1]);
        String rootPath = split[2];
        Map<String, String> configMap = removePrefix(config.getOriginOptions());
        starrocksWriter = new StarrocksWriter(tabletId, pbSchema, txnId, rootPath, configMap);
        starrocksWriter.open();

        isFirstRecord = false;
    }

    private void internalRow2Chunk(InternalRow internalRow) {
        int dstTableColumnSize = internalRow.numFields() - 1;
        List<EtlJobConfig.EtlColumn> columns = schema.getEtlTable().indexes.get(0).getColumns();
        assert dstTableColumnSize == columns.size();
        for (int i = 0; i < columns.size(); i++) {
            Column column = chunk.getColumn(i);

            if (internalRow.isNullAt(i)) {
                column.appendNull();
                continue;
            }
            switch (columns.get(i).columnType) {
                case "INT":
                    column.appendInt(internalRow.getInt(i));
                    break;
                case "BOOLEAN":
                    column.appendBool(true);
                    break;
                case "TINYINT":
                    column.appendByte((byte) internalRow.getInt(i));
                    break;
                case "SMALLINT":
                    column.appendShort(internalRow.getShort(i));
                    break;
                case "BIGINT":
                    column.appendLong(internalRow.getLong(i));
                    break;
                case "FLOAT":
                    column.appendFloat(internalRow.getFloat(i));
                    break;
                case "DOUBLE":
                    column.appendDouble(internalRow.getDouble(i));
                    break;
                case "VARCHAR":
                    column.appendString(internalRow.getString(i));
                    break;
                case "DATE":
                    column.appendDate(DateTimeUtils.toJavaDate(internalRow.getInt(i)));
                    break;
                case "DATETIME":
                    column.appendTimestamp(DateTimeUtils.toJavaTimestamp(internalRow.getLong(i)));
                    break;
                default:
                    throw new RuntimeException("write not support current type:" + columns.get(i).columnType);
            }
        }
    }
}
