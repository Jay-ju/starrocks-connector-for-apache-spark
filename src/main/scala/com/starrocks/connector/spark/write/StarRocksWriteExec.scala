// Modifications Copyright 2021 StarRocks Limited.
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

package org.apache.spark.sql.execution.datasources.v2

import com.starrocks.connector.spark.sql.preprocessor.EtlJobConfig.EtlIndex
import com.starrocks.connector.spark.sql.preprocessor._
import com.starrocks.connector.spark.sql.write.StarRocksWrite
import org.apache.spark.TaskContext
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write.{BatchWrite, PhysicalWriteInfoImpl, WriterCommitMessage}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.LongAccumulator

import java.util
import java.util.List
import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}
import scala.util.control.NonFatal

case class StarRocksWriteExec(batchWrite: BatchWrite,
                          query: SparkPlan,
                          writeMetrics: Seq[CustomMetric]) extends V2TableWriteExec {

  override val customMetrics: Map[String, SQLMetric] = writeMetrics.map { customMetric =>
    customMetric.name() -> SQLMetrics.createV2CustomMetric(sparkContext, customMetric)
  }.toMap

  override protected def run(): Seq[InternalRow] = {
    val rdd: RDD[InternalRow] = {
      val tempRdd = query.execute()
      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      if (tempRdd.partitions.length == 0) {
        sparkContext.parallelize(Array.empty[InternalRow], 1)
      } else {
        tempRdd
      }
    }
    val newRdd = processRDD(batchWrite, rdd)

    val writerFactory = batchWrite.createBatchWriterFactory(
      PhysicalWriteInfoImpl(newRdd.getNumPartitions))
    val useCommitCoordinator = batchWrite.useCommitCoordinator
    val messages = new Array[WriterCommitMessage](newRdd.partitions.length)
    val totalNumRowsAccumulator = new LongAccumulator()

    logInfo(s"Start processing data source write support: $batchWrite. " +
      s"The input RDD has ${messages.length} partitions.")

    // Avoid object not serializable issue.
    val writeMetrics: Map[String, SQLMetric] = customMetrics

    try {
      sparkContext.runJob(
        newRdd,
        (context: TaskContext, iter: Iterator[InternalRow]) =>
          DataWritingSparkTask.run(writerFactory, context, iter, useCommitCoordinator,
            writeMetrics),
        newRdd.partitions.indices,
        (index, result: DataWritingSparkTaskResult) => {
          val commitMessage = result.writerCommitMessage
          messages(index) = commitMessage
          totalNumRowsAccumulator.add(result.numRows)
          batchWrite.onDataWriterCommit(commitMessage)
        }
      )

      logInfo(s"Data source write support $batchWrite is committing.")
      batchWrite.commit(messages)
      logInfo(s"Data source write support $batchWrite committed.")
      commitProgress = Some(StreamWriterCommitProgress(totalNumRowsAccumulator.value))
    } catch {
      case cause: Throwable =>
        logError(s"Data source write support $batchWrite is aborting.")
        try {
          batchWrite.abort(messages)
        } catch {
          case t: Throwable =>
            logError(s"Data source write support $batchWrite failed to abort.")
            cause.addSuppressed(t)
            throw QueryExecutionErrors.writingJobFailedError(cause)
        }
        logError(s"Data source write support $batchWrite aborted.")
        cause match {
          // Only wrap non fatal exceptions.
          case NonFatal(e) => throw QueryExecutionErrors.writingJobAbortedError(e)
          case _ => throw cause
        }
    }

    Nil
  }

  override protected def withNewChildInternal(newChild: SparkPlan): StarRocksWriteExec =
    copy(query = newChild)


  protected def processRDD(batchWrite: BatchWrite, rdd: RDD[InternalRow]): RDD[InternalRow] = {
    if (!batchWrite.isInstanceOf[StarRocksWrite]) {
      throw new Exception("Need check that spark match starrocks")
    }

    val schema = batchWrite.asInstanceOf[StarRocksWrite].getSchema;

    val fileGroupPartitions = new util.ArrayList[EtlJobConfig.EtlPartition]()
    val fileGroupPartitionRangeKeys = new util.ArrayList[StarRocksRangePartitioner.PartitionRangeKey]()
    val partitionInfo = schema.getEtlTable.partitionInfo
    val partitionSize: Int = partitionInfo.partitions.size
    val preProcessor = new StarRocksPreProcessor(sparkContext.hadoopConfiguration)
    val baseIndex = fillRollupIndexes(schema.getEtlTable.indexes)
    val partitionKeySchema = fillPartitionSchema(baseIndex, schema.getEtlTable.partitionInfo.partitionColumnRefs)
    val partitionRangeKeys = preProcessor.createPartitionRangeKeys(partitionInfo, partitionKeySchema.schema)
    val columnNames = fillColumnNames(baseIndex)
    val dstTableSchema = DppUtils.createDstTableSchema(baseIndex.columns, false, false)
    val rollupTreeParser = new MinimumCoverageRollupTreeBuilder
    val rootNode = rollupTreeParser.build(schema.getEtlTable)
    val bucketKeyMap = new util.HashMap[String, Integer] {}
    val partitionKeyMap = new util.HashMap[java.lang.Long, EtlJobConfig.EtlPartition] {}
    var reduceNum = 0

    for (partitionIdx <- 0 until partitionSize) {
      val partition: EtlJobConfig.EtlPartition = partitionInfo.partitions.get(partitionIdx)
      partitionKeyMap.put(partition.partitionId, partition)
      for (bucketIdx <- 0 until partition.bucketNum) {
        bucketKeyMap.put(partition.partitionId + "_" + bucketIdx, reduceNum)
        reduceNum += 1
      }
      fileGroupPartitions.add(partition)
      fileGroupPartitionRangeKeys.add(partitionRangeKeys.get(partitionIdx))
    }

    val tablePairRDD: JavaPairRDD[List[AnyRef], Array[AnyRef]] = preProcessor.fillTupleWithPartitionColumn(
      partitionInfo, rdd, partitionKeySchema.index, fileGroupPartitionRangeKeys,
      columnNames.keys, columnNames.values, dstTableSchema, baseIndex)

    preProcessor.processRollupTree(rootNode, tablePairRDD, baseIndex, bucketKeyMap, partitionKeyMap)
  }

  def fillPartitionSchema(baseIndex: EtlJobConfig.EtlIndex, partitionColumnRefs: List[String]): PartitionKeySchema = {
    val keySchema: List[Class[_]] = new util.ArrayList[Class[_]]
    val keyIndex: util.List[Integer] = new util.ArrayList[Integer]
    for (partitionColName <- partitionColumnRefs.asScala) {
      breakable {
        for (i <- 0 until baseIndex.columns.size) {
          val column: EtlJobConfig.EtlColumn = baseIndex.columns.get(i)
          if (column.columnName == partitionColName) {
            keyIndex.add(i)
            keySchema.add(DppUtils.getClassFromColumn(column))
            break
          }
        }
      }
    }
    PartitionKeySchema(keyIndex, keySchema)
  }

  def fillRollupIndexes(indexes: List[EtlIndex]) : EtlJobConfig.EtlIndex = {
    // get the base index meta
    var baseIndex: EtlJobConfig.EtlIndex = null
    breakable {
      for (indexMeta: EtlJobConfig.EtlIndex <- indexes.asScala) {
        if (indexMeta.isBaseIndex) {
          baseIndex = indexMeta
          break
        }
      }
    }
    baseIndex
  }

  def fillColumnNames(baseIndex: EtlJobConfig.EtlIndex) : ColumnName = {
    // get key column names and value column names separately
    val keyColumnNames: List[String] = new util.ArrayList[String]
    val valueColumnNames: List[String] = new util.ArrayList[String]
    for (etlColumn: EtlJobConfig.EtlColumn  <- baseIndex.columns.asScala) {
      if (etlColumn.isKey) {
        keyColumnNames.add(etlColumn.columnName)
      }
      else {
        valueColumnNames.add(etlColumn.columnName)
      }
    }
    ColumnName(keyColumnNames, valueColumnNames)
  }

  case class PartitionKeySchema(index: util.List[Integer], schema: util.List[Class[_]])
  case class ColumnName(keys: util.List[String], values: util.List[String])
}