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

package com.starrocks.connector.spark.sql.preprocessor;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.SerializableConfiguration;
import scala.Tuple2;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.spark.sql.types.DataTypes.StringType;

// This class is a Spark-based data preprocessing program,
// which will make use of the distributed compute framework of spark to
// do ETL job/sort/preaggregate jobs in spark job
// to boost the process of large amount of data load.
// the process steps are as following:
// 1. load data
//     1.1 load data from path/hive table
//     1.2 do the etl process
// 2. repartition data by using starrocks data model(partition and bucket)
// 3. process aggregation if needed
// 4. write data to parquet file
public final class StarRocksPreProcessor implements java.io.Serializable {
    private static final Logger LOG = LogManager.getLogger(StarRocksPreProcessor.class);

    // save the hadoop configuration from spark session.
    // because hadoop configuration is not serializable,
    // we need to wrap it so that we can use it in executor.
    private SerializableConfiguration serializableHadoopConf;

    // just for ut
    public StarRocksPreProcessor(Configuration hadoopConf) {
        serializableHadoopConf = new SerializableConfiguration(hadoopConf);
    }

    // write data to parquet file by using writing the parquet scheme of spark.
    public JavaPairRDD repartitionAndSortedRDD(JavaPairRDD<List<Object>, Object[]> resultRDD,
                                               Map<String, Integer> bucketKeyMap) {
        // TODO(wb) should deal largeint as BigInteger instead of string when using biginteger as key,
        // data type may affect sorting logic
        return resultRDD.repartitionAndSortWithinPartitions(new BucketPartitioner(bucketKeyMap),
                new BucketComparator());
    }

    public JavaRDD<InternalRow> processPartition(JavaPairRDD rdd,
                                                 EtlJobConfig.EtlIndex indexMeta,
                                                 SparkRDDAggregator[] sparkRDDAggregators,
                                                 Map<Long, EtlJobConfig.EtlPartition> partitionKeyMap) {
        StructType tableSchema = DppUtils.createDstTableSchema(indexMeta.columns, false, true);

        // add tablet column
        StructType internalTableSchema = tableSchema.add("tablet_id", StringType);

        ExpressionEncoder encoder = RowEncoder.apply(internalTableSchema);
        return rdd.mapPartitionsWithIndex((Function2<Integer, Iterator<Tuple2<List<Object>, Object[]>>, Iterator>) (idx, t) -> {
            List<InternalRow> internalRows = new ArrayList<>();
            while (t.hasNext()) {
                String lastBucketKey = null;
                TaskContext taskContext = TaskContext.get();
                Tuple2<List<Object>, Object[]> pair = t.next();
                List<Object> keyColumns = pair._1();
                Object[] valueColumns = pair._2();
                if ((keyColumns.size() + valueColumns.length) <= 1) {
                    LOG.warn("invalid row:" + pair);
                }

                String curBucketKey = keyColumns.get(0).toString();
                List<Object> allColumnPerRow = new ArrayList<>();
                for (int i = 1; i < keyColumns.size(); ++i) {
                    allColumnPerRow.add(keyColumns.get(i));
                }
                // key first element is partition_bucket
                int keySize = keyColumns.size() - 1;
                for (int i = 0; i < valueColumns.length; ++i) {
                    allColumnPerRow.add(sparkRDDAggregators[i].finalize(valueColumns[i]));
                }

                Long tabletId = -1L;
                Long backendId = -1L;
                String storagePath = "";
                // if the bucket key is new, it will belong to a new tablet
                if (lastBucketKey == null || !curBucketKey.equals(lastBucketKey)) {
                    // flush current writer and create a new writer
                    String[] bucketKey = curBucketKey.split("_");
                    if (bucketKey.length != 2) {
                        LOG.warn("invalid bucket key:" + curBucketKey);
                    }
                    Long partitionId = Long.parseLong(bucketKey[0]);
                    int bucketId = Integer.parseInt(bucketKey[1]);
                    lastBucketKey = curBucketKey;
                    // must order by asc
                    tabletId = partitionKeyMap.get(partitionId).getTabletIds().get(bucketId);
                    backendId = partitionKeyMap.get(partitionId).getBackendIds().get(bucketId);
                    storagePath = partitionKeyMap.get(partitionId).getStoragePath();
                }
                String partitionExtension = String.join("\u0001", String.valueOf(tabletId), String.valueOf(backendId), storagePath);
                allColumnPerRow.add(partitionExtension);
                Row rowWithTabletId = RowFactory.create(allColumnPerRow.toArray());

                ExpressionEncoder.Serializer toRow = encoder.createSerializer();
                internalRows.add(toRow.apply(rowWithTabletId));
            }
            return internalRows.iterator();
        }, true);

    }

    // TODO(wb) one shuffle to calculate the rollup in the same level
    public JavaRDD<InternalRow> processRollupTree(RollupTreeNode rootNode,
                                                  JavaPairRDD<List<Object>, Object[]> rootRDD,
                                                  EtlJobConfig.EtlIndex baseIndex,
                                                  Map<String, Integer> bucketKeyMap,
                                                  Map<Long, EtlJobConfig.EtlPartition> partitionKeyMap) throws SparkWriteSDKException {
        Queue<RollupTreeNode> nodeQueue = new LinkedList<>();
        nodeQueue.offer(rootNode);
        int currentLevel = 0;
        // level travel the tree
        Map<Long, JavaPairRDD<List<Object>, Object[]>> parentRDDMap = new HashMap<>();
        parentRDDMap.put(baseIndex.indexId, rootRDD);
        Map<Long, JavaPairRDD<List<Object>, Object[]>> childrenRDDMap = new HashMap<>();
        while (!nodeQueue.isEmpty()) {
            RollupTreeNode curNode = nodeQueue.poll();
            LOG.info("start to process index:" + curNode.indexId);
            if (curNode.children != null) {
                for (RollupTreeNode child : curNode.children) {
                    nodeQueue.offer(child);
                }
            }
            JavaPairRDD<List<Object>, Object[]> curRDD = null;
            // column select for rollup
            if (curNode.level != currentLevel) {
                for (JavaPairRDD<List<Object>, Object[]> rdd : parentRDDMap.values()) {
                    rdd.unpersist();
                }
                currentLevel = curNode.level;
                parentRDDMap.clear();
                parentRDDMap = childrenRDDMap;
                childrenRDDMap = new HashMap<>();
            }

            long parentIndexId = baseIndex.indexId;
            if (curNode.parent != null) {
                parentIndexId = curNode.parent.indexId;
            }

            JavaPairRDD<List<Object>, Object[]> parentRDD = parentRDDMap.get(parentIndexId);

            // aggregate
            SparkRDDAggregator[] sparkRDDAggregators = new SparkRDDAggregator[curNode.valueColumnNames.size()];
            curRDD = processRDDAggregate(parentRDD, curNode, sparkRDDAggregators);

            childrenRDDMap.put(curNode.indexId, curRDD);

            if (curNode.children != null && curNode.children.size() > 1) {
                // if the children number larger than 1, persist the dataframe for performance
                curRDD.persist(StorageLevel.MEMORY_AND_DISK());
            }
            JavaPairRDD pairRDD = repartitionAndSortedRDD(curRDD, bucketKeyMap);
            return processPartition(pairRDD, baseIndex, sparkRDDAggregators, partitionKeyMap);
        }
        return new JavaSparkContext().emptyRDD();
    }

    private JavaPairRDD<List<Object>, Object[]> processRDDAggregate(JavaPairRDD<List<Object>, Object[]> currentPairRDD,
                                                                    RollupTreeNode curNode,
                                                                    SparkRDDAggregator[] sparkRDDAggregators)
            throws SparkWriteSDKException {
        final boolean isDuplicateTable = !StringUtils.equalsIgnoreCase(curNode.indexMeta.indexType, "AGGREGATE")
                && !StringUtils.equalsIgnoreCase(curNode.indexMeta.indexType, "UNIQUE");

        // Aggregate/UNIQUE table
        if (!isDuplicateTable) {
            // TODO(wb) set the reduce concurrency by statistic instead of hard code 200
            int aggregateConcurrency = 200;

            int idx = 0;
            for (int i = 0; i < curNode.indexMeta.columns.size(); i++) {
                if (!curNode.indexMeta.columns.get(i).isKey) {
                    sparkRDDAggregators[idx] = SparkRDDAggregator.buildAggregator(curNode.indexMeta.columns.get(i));
                    idx++;
                }
            }

            if (curNode.indexMeta.isBaseIndex) {
                JavaPairRDD<List<Object>, Object[]> result =
                        currentPairRDD.mapToPair(new EncodeBaseAggregateTableFunction(sparkRDDAggregators))
                                .reduceByKey(new AggregateReduceFunction(sparkRDDAggregators), aggregateConcurrency);
                return result;
            } else {
                JavaPairRDD<List<Object>, Object[]> result = currentPairRDD
                        .mapToPair(new EncodeRollupAggregateTableFunction(
                                getColumnIndexInParentRollup(curNode.keyColumnNames, curNode.valueColumnNames,
                                        curNode.parent.keyColumnNames,
                                        curNode.parent.valueColumnNames)))
                        .reduceByKey(new AggregateReduceFunction(sparkRDDAggregators), aggregateConcurrency);
                return result;
            }
            // Duplicate Table
        } else {
            int idx = 0;
            for (int i = 0; i < curNode.indexMeta.columns.size(); i++) {
                if (!curNode.indexMeta.columns.get(i).isKey) {
                    // duplicate table doesn't need aggregator
                    // init a aggregator here just for keeping interface compatibility when writing data to HDFS
                    sparkRDDAggregators[idx] = new DefaultSparkRDDAggregator();
                    idx++;
                }
            }
            if (curNode.indexMeta.isBaseIndex) {
                return currentPairRDD;
            } else {
                return currentPairRDD.mapToPair(new EncodeRollupAggregateTableFunction(
                        getColumnIndexInParentRollup(curNode.keyColumnNames, curNode.valueColumnNames,
                                curNode.parent.keyColumnNames, curNode.parent.valueColumnNames)));
            }
        }
    }

    // get column index map from parent rollup to child rollup
    // not consider bucketId here
    private Pair<Integer[], Integer[]> getColumnIndexInParentRollup(List<String> childRollupKeyColumns,
                                                                    List<String> childRollupValueColumns,
                                                                    List<String> parentRollupKeyColumns,
                                                                    List<String> parentRollupValueColumns)
            throws SparkWriteSDKException {
        List<String> parentRollupColumns = new ArrayList<>();
        parentRollupColumns.addAll(parentRollupKeyColumns);
        parentRollupColumns.addAll(parentRollupValueColumns);

        List<Integer> keyMap = getChildColumnIds(childRollupKeyColumns, parentRollupColumns);
        List<Integer> valueMap = getChildColumnIds(childRollupValueColumns, parentRollupColumns);

        if (keyMap.size() != childRollupKeyColumns.size() || valueMap.size() != childRollupValueColumns.size()) {
            throw new SparkWriteSDKException(String.format(
                    "column map index from child to parent has error, key size src: %s, dst: %s; value size src: %s, dst: %s",
                    childRollupKeyColumns.size(), keyMap.size(), childRollupValueColumns.size(), valueMap.size()));
        }

        return Pair.of(keyMap.toArray(new Integer[keyMap.size()]), valueMap.toArray(new Integer[valueMap.size()]));
    }

    private List<Integer> getChildColumnIds(List<String> childRollupColumns, List<String> parentRollupColumns) {
        List<Integer> childColumnIds = new ArrayList<>();

        for (int i = 0; i < childRollupColumns.size(); i++) {
            for (int j = 0; j < parentRollupColumns.size(); j++) {
                if (StringUtils.equalsIgnoreCase(childRollupColumns.get(i), parentRollupColumns.get(j))) {
                    childColumnIds.add(j);
                    break;
                }
            }
        }

        return childColumnIds;
    }

    /**
     * check decimal,char/varchar
     */
    public boolean validateData(Object srcValue, EtlJobConfig.EtlColumn etlColumn, ColumnParser columnParser,
                                InternalRow row) {

        switch (etlColumn.columnType.toUpperCase()) {
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
                // TODO(wb):  support decimal round; see be DecimalV2Value::round
                DecimalParser decimalParser = (DecimalParser) columnParser;
                BigDecimal srcDecimal = ((Decimal) srcValue).toBigDecimal().bigDecimal();
                if (srcValue != null && (decimalParser.getMaxValue().compareTo(srcDecimal) < 0 ||
                        decimalParser.getMinValue().compareTo(srcDecimal) > 0)) {
                    LOG.warn(String.format(
                            "decimal value is not valid for defination, column=%s, value=%s,precision=%s,scale=%s",
                            etlColumn.columnName, srcValue.toString(), srcDecimal.precision(),
                            srcDecimal.scale()));
                    return false;
                }
                break;
            case "CHAR":
            case "VARCHAR":
                // TODO(wb) padding char type
                int strSize = 0;
                if (srcValue != null &&
                        (strSize = srcValue.toString().getBytes(StandardCharsets.UTF_8).length) >
                                etlColumn.stringLength) {
                    LOG.warn(String.format(
                            "the length of input is too long than schema. column_name:%s," +
                                    "input_str[%s],schema length:%s,actual length:%s",
                            etlColumn.columnName, row.toString(), etlColumn.stringLength, strSize));
                    return false;
                }
                break;
        }
        return true;
    }

    /**
     * 1 project column and reorder column
     * 2 validate data
     * 3 fill tuple with partition column
     */
    public JavaPairRDD<List<Object>, Object[]> fillTupleWithPartitionColumn(
            EtlJobConfig.EtlPartitionInfo partitionInfo,
            RDD<InternalRow> rdd,
            List<Integer> partitionKeyIndex,
            List<StarRocksRangePartitioner.PartitionRangeKey> partitionRangeKeys,
            List<String> keyColumnNames,
            List<String> valueColumnNames,
            StructType dstTableSchema,
            EtlJobConfig.EtlIndex baseIndex) throws SparkWriteSDKException {
        List<String> distributeColumns = partitionInfo.distributionColumnRefs;
        Partitioner partitioner = new StarRocksRangePartitioner(partitionInfo, partitionKeyIndex, partitionRangeKeys);

        List<ColumnParser> parsers = new ArrayList<>();
        for (EtlJobConfig.EtlColumn column : baseIndex.columns) {
            parsers.add(ColumnParser.create(column));
        }

        // use PairFlatMapFunction instead of PairMapFunction because there will be
        // 0 or 1 output row for 1 input row
        JavaPairRDD<List<Object>, Object[]> resultPairRDD =
                rdd.toJavaRDD().flatMapToPair((PairFlatMapFunction<InternalRow, List<Object>, Object[]>) row -> {
                    List<Tuple2<List<Object>, Object[]>> result = new ArrayList<>();
                    RowContext rowContext = new RowContext();
                    boolean validData =
                            rowContext.processRow(keyColumnNames, row, dstTableSchema, baseIndex, parsers,
                                    true, keyColumnNames.size()) &&
                            rowContext.processRow(valueColumnNames, row, dstTableSchema, baseIndex, parsers,
                                    false, keyColumnNames.size());
                    if (!validData) {
                        return result.iterator();
                    }

                    int pid = partitioner.getPartition(new DppColumns(rowContext.getAllColumnObjects()));
                    if (pid < 0) {
                        LOG.warn("invalid partition for row:" + row + ", abnormal rows num:");
                    } else {
                        // TODO(wb) support large int for hash
                        long hashValue = DppUtils.getHashValue(row, distributeColumns, dstTableSchema);
                        int bucketId =
                                (int) ((hashValue & 0xffffffff) % partitionInfo.partitions.get(pid).bucketNum);
                        long partitionId = partitionInfo.partitions.get(pid).partitionId;
                        // bucketKey is partitionId_bucketId
                        String bucketKey = partitionId + "_" + bucketId;

                        List<Object> tuple = new ArrayList<>();
                        tuple.add(bucketKey);
                        tuple.addAll(rowContext.getKeyColumnObjects());
                        result.add(new Tuple2<>(tuple, rowContext.getValueColumnObjects().toArray()));
                    }
                    return result.iterator();
                });

        return resultPairRDD;
    }

    public class RowContext {
        private List<Object> keyColumnObjects = new ArrayList<>();
        private List<Object> valueColumnObjects = new ArrayList<>();
        private List<Object> allColumnObjects = new ArrayList<>();

        public RowContext() {}

        public RowContext(List<Object> keyColumnObjects,
                          List<Object> valueColumnObjects,
                          List<Object> allColumnObjects) {
            this.keyColumnObjects = keyColumnObjects;
            this.valueColumnObjects = valueColumnObjects;
            this.allColumnObjects = allColumnObjects;
        }

        public List<Object> getKeyColumnObjects() {
            return keyColumnObjects;
        }

        public List<Object> getValueColumnObjects() {
            return valueColumnObjects;
        }

        public List<Object> getAllColumnObjects() {
            return allColumnObjects;
        }

        public boolean processRow(List<String> columnNames,
                                  InternalRow row,
                                  StructType tableSchema,
                                  EtlJobConfig.EtlIndex baseIndex,
                                  List<ColumnParser> parsers,
                                  boolean isKey,
                                  int keySize) {
            for (int i = 0; i < columnNames.size(); i++) {
                String columnName = columnNames.get(i);
                Object columnObject = row.get((int) tableSchema.getFieldIndex(columnName).get(),
                        tableSchema.apply(columnName).dataType());
                int parserIdx = i + (isKey ? 0 : keySize);
                if (!validateData(columnObject, baseIndex.getColumn(columnName), parsers.get(parserIdx), row)) {
                    return false;
                }
                columnObject = convertToJavaType(columnObject, tableSchema.apply(columnName).dataType());
                if (isKey) {
                    keyColumnObjects.add(columnObject);
                } else {
                    valueColumnObjects.add(columnObject);
                }
                allColumnObjects.add(columnObject);
            }
            return true;
        }
    }

    private Object convertToJavaType(Object value, DataType dataType) {
        if (value instanceof UTF8String && dataType == StringType) {
            return ((UTF8String) value).toString();
        }
        return value;
    }

    // partition keys will be parsed into double from json
    // so need to convert it to partition columns' type
    private Object convertPartitionKey(Object srcValue, Class dstClass) throws SparkWriteSDKException {
        if (dstClass.equals(Float.class) || dstClass.equals(Double.class)) {
            return null;
        }

        // PartitionKey is initialized according to the value of Json deserialization,
        // because the data type is Double after deserialization,
        // so there will be a conditional judgment of "if (srcValue instanceof Double)"
        if (srcValue instanceof Double) {
            if (dstClass.equals(Short.class)) {
                return ((Double) srcValue).shortValue();
            } else if (dstClass.equals(Integer.class)) {
                return ((Double) srcValue).intValue();
            } else if (dstClass.equals(Long.class)) {
                return ((Double) srcValue).longValue();
            } else if (dstClass.equals(BigInteger.class)) {
                // TODO(wb) gson will cast origin value to double by default
                // when the partition column is largeint, this will cause error data
                // need fix it thoroughly
                return new BigInteger(((Double) srcValue).toString());
            } else if (dstClass.equals(java.sql.Date.class) || dstClass.equals(java.util.Date.class)) {
                double srcValueDouble = (double) srcValue;
                return convertToJavaDate((int) srcValueDouble);
            } else if (dstClass.equals(java.sql.Timestamp.class)) {
                double srcValueDouble = (double) srcValue;
                return convertToJavaDatetime((long) srcValueDouble);
            } else {
                // dst type is string
                return srcValue.toString();
            }
        } else {
            LOG.warn("unsupport partition key:" + srcValue);
            throw new SparkWriteSDKException("unsupport partition key:" + srcValue);
        }
    }

    private java.sql.Timestamp convertToJavaDatetime(long src) {
        String dateTimeStr = Long.valueOf(src).toString();
        if (dateTimeStr.length() != 14) {
            throw new RuntimeException("invalid input date format for SparkDpp");
        }

        String year = dateTimeStr.substring(0, 4);
        String month = dateTimeStr.substring(4, 6);
        String day = dateTimeStr.substring(6, 8);
        String hour = dateTimeStr.substring(8, 10);
        String min = dateTimeStr.substring(10, 12);
        String sec = dateTimeStr.substring(12, 14);

        return java.sql.Timestamp.valueOf(String.format("%s-%s-%s %s:%s:%s", year, month, day, hour, min, sec));
    }

    private java.sql.Date convertToJavaDate(int originDate) {
        int day = originDate & 0x1f;
        originDate >>= 5;
        int month = originDate & 0x0f;
        originDate >>= 4;
        int year = originDate;
        return java.sql.Date.valueOf(String.format("%04d-%02d-%02d", year, month, day));
    }

    public List<StarRocksRangePartitioner.PartitionRangeKey> createPartitionRangeKeys(
            EtlJobConfig.EtlPartitionInfo partitionInfo, List<Class> partitionKeySchema) throws SparkWriteSDKException {
        List<StarRocksRangePartitioner.PartitionRangeKey> partitionRangeKeys = new ArrayList<>();
        for (EtlJobConfig.EtlPartition partition : partitionInfo.partitions) {
            StarRocksRangePartitioner.PartitionRangeKey partitionRangeKey =
                    new StarRocksRangePartitioner.PartitionRangeKey();

            if (!partition.isMinPartition) {
                partitionRangeKey.isMinPartition = false;
                List<Object> startKeyColumns = new ArrayList<>();
                for (int i = 0; i < partition.startKeys.size(); i++) {
                    Object value = partition.startKeys.get(i);
                    startKeyColumns.add(convertPartitionKey(value, partitionKeySchema.get(i)));
                }
                partitionRangeKey.startKeys = new DppColumns(startKeyColumns);
            } else {
                partitionRangeKey.isMinPartition = true;
            }

            if (!partition.isMaxPartition) {
                partitionRangeKey.isMaxPartition = false;
                List<Object> endKeyColumns = new ArrayList<>();
                for (int i = 0; i < partition.endKeys.size(); i++) {
                    Object value = partition.endKeys.get(i);
                    endKeyColumns.add(convertPartitionKey(value, partitionKeySchema.get(i)));
                }
                partitionRangeKey.endKeys = new DppColumns(endKeyColumns);
            } else {
                partitionRangeKey.isMaxPartition = true;
            }

            partitionRangeKeys.add(partitionRangeKey);
        }
        return partitionRangeKeys;
    }

}

