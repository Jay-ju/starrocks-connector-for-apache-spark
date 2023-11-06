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

package com.starrocks.connector.spark.write

import com.starrocks.connector.spark.sql.StarRocksTable
import com.starrocks.connector.spark.sql.write.StarRocksWriteBuilder.StarRocksWriteImpl
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan}
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, StarRocksWriteExec, WriteToDataSourceV2}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

case class StarRocksWriteStrategy(spark: SparkSession) extends SparkStrategy with Logging{
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case AppendData(r @ DataSourceV2Relation(v1: SupportsWrite, _, _, _, _), query, _, _, Some(write))
      if v1.isInstanceOf[StarRocksTable] && write.isInstanceOf[StarRocksWriteImpl] =>
      StarRocksWriteExec(write.toBatch, planLater(query), Nil) :: Nil

    case WriteToDataSourceV2(_, writer, query, customMetrics) =>
      StarRocksWriteExec(writer, planLater(query), customMetrics) :: Nil
    case _ => Nil
  }

}