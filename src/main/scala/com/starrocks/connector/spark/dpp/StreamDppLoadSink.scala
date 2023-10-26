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

package com.starrocks.connector.spark.dpp

import com.starrocks.connector.spark.sql.dpp._
import com.starrocks.connector.spark.sql.schema.StarRocksSchema
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext}

import java.util
import java.util.List
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.util.control.Breaks.{break, breakable}

class StreamDppLoadSink(sqlContext: SQLContext, schema: StarRocksSchema)
  extends Sink with Serializable {

  @volatile private var latestBatchId = -1L

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      latestBatchId = batchId
    }
  }

  def write(data: DataFrame): Unit = {
    val fileGroupPartitions = new util.ArrayList[EtlJobConfig.EtlPartition]()
    val fileGroupPartitionRangeKeys = new util.ArrayList[StarRocksRangePartitioner.PartitionRangeKey]()
    val partitionInfo = schema.getEtlTable.partitionInfo
    val partitionSize: Int = partitionInfo.partitions.size
    val sparkDpp = new SparkDpp(sqlContext.sparkContext.hadoopConfiguration)
    val baseIndex = fillRollupIndexs
    val partitionKeySchema = fillPartitionSchema(baseIndex)
    val partitionRangeKeys = sparkDpp.createPartitionRangeKeys(partitionInfo, partitionKeySchema.schema)
    val columnNames = fillColumnNames(baseIndex)
    val dstTableSchema = DppUtils.createDstTableSchema(baseIndex.columns, false, false)
    val rollupTreeParser = new MinimumCoverageRollupTreeBuilder
    val rootNode = rollupTreeParser.build(schema.getEtlTable)
    val bucketKeyMap:util.Map[String, Integer] = new util.HashMap[String, Integer] {}
    var reduceNum = 0

    for (i <- 0 until partitionSize) {
      val partition: EtlJobConfig.EtlPartition = partitionInfo.partitions.get(i)
      for (i <- 0 until partition.bucketNum) {
        bucketKeyMap.put(partition.partitionId + "_" + i, reduceNum)
        reduceNum += 1
      }
      fileGroupPartitions.add(partition)
      fileGroupPartitionRangeKeys.add(partitionRangeKeys.get(i))
    }
    val fileGroupPartitionInfo: EtlJobConfig.EtlPartitionInfo =
      new EtlJobConfig.EtlPartitionInfo(partitionInfo.partitionType, partitionInfo.partitionColumnRefs,
        partitionInfo.distributionColumnRefs, fileGroupPartitions)

    val tablePairRDD: JavaPairRDD[List[AnyRef], Array[AnyRef]] = sparkDpp.fillTupleWithPartitionColumn(
      data, fileGroupPartitionInfo, partitionKeySchema.index, fileGroupPartitionRangeKeys,
      columnNames.keys, columnNames.values, dstTableSchema, baseIndex)

    // repartition and write to hdfs
    sparkDpp.processRollupTree(rootNode, tablePairRDD, schema.getTableId, baseIndex, bucketKeyMap)
  }

  override def toString: String = "StreamDppLoadSink"

  def fillPartitionSchema(baseIndex: EtlJobConfig.EtlIndex): PartitionKeySchema = {
    val keySchema: List[Class[_]] = new util.ArrayList[Class[_]]
    val keyIndex: util.List[Integer] = new util.ArrayList[Integer]
    for (partitionColName <- schema.getEtlTable.partitionInfo.partitionColumnRefs.asScala) {
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

  def fillRollupIndexs() : EtlJobConfig.EtlIndex = {
    // get the base index meta
    var baseIndex: EtlJobConfig.EtlIndex = null
    breakable {
      for (indexMeta: EtlJobConfig.EtlIndex <- schema.getEtlTable.indexes.asScala) {
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
}

case class PartitionKeySchema(index: util.List[Integer], schema: util.List[Class[_]])
case class ColumnName(keys: util.List[String], values: util.List[String])
