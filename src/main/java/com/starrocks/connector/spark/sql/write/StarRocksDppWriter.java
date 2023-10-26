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

import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.util.EnvUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class StarRocksDppWriter extends StarRocksWriter {
    private static final Logger log = LoggerFactory.getLogger(StarRocksDppWriter.class);

    private final WriteStarRocksConfig config;
    private final int partitionId;
    private final long taskId;
    private final long epochId;

    public StarRocksDppWriter(WriteStarRocksConfig config,
                              int partitionId,
                              long taskId,
                              long epochId) {
        this.config = config;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
    }

    @Override
    public void open() {
        log.info("Open data writer for partition: {}, task: {}, epoch: {}, {}",
                partitionId, taskId, epochId, EnvUtils.getGitInformation());
    }

    @Override
    public void write(InternalRow internalRow) throws IOException {
        log.debug("partitionId: {}, taskId: {}, epochId: {}, receive raw row: {}",
                partitionId, taskId, epochId, internalRow);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        log.info("partitionId: {}, taskId: {}, epochId: {} commit", partitionId, taskId, epochId);
        return null;
    }

    @Override
    public void abort() throws IOException {
        log.info("partitionId: {}, taskId: {}, epochId: {} abort", partitionId, taskId, epochId);
    }

    @Override
    public void close() throws IOException {
        log.info("partitionId: {}, taskId: {}, epochId: {} close", partitionId, taskId, epochId);
    }
}
