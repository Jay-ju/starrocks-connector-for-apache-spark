package com.starrocks.connector.spark.sql.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;

import java.io.Serializable;

public abstract class StarRocksWriter implements DataWriter<InternalRow>, Serializable {
    void open() {

    }
}
