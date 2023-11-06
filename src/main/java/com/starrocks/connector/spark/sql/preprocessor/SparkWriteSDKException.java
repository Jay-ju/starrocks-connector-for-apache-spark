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

import com.google.common.base.Strings;

// Exception for Spark DPP process
public class SparkWriteSDKException extends Exception {
    public SparkWriteSDKException(String msg, Throwable cause) {
        super(Strings.nullToEmpty(msg), cause);
    }

    public SparkWriteSDKException(Throwable cause) {
        super(cause);
    }

    public SparkWriteSDKException(String msg, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(Strings.nullToEmpty(msg), cause, enableSuppression, writableStackTrace);
    }

    public SparkWriteSDKException(String msg) {
        super(Strings.nullToEmpty(msg));
    }
}
