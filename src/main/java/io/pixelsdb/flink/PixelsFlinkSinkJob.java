/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pixelsdb.flink;

import io.pixelsdb.pixels.sink.SinkProto;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.util.ParameterTool;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog; // 使用 GlueCatalog
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.flink.table.data.RowData;

import org.apache.iceberg.catalog.TableIdentifier;
import java.util.List; // 导入 List 用于获取等值字段
import java.util.Set;


public class PixelsFlinkSinkJob
{
    // 定义常量以避免硬编码参数名
    private static final String CATALOG_NAME = "catalog.name";


    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // --- 1. 参数校验和获取 ---

        // 确保传递了所有必要的参数
        params.getRequired("source");
        String serverHost = params.getRequired("source.server.host");
        // 假设默认端口为 8080
        int serverPort = params.getInt("source.server.port", 8080);
        String databaseName = params.getRequired("source.database.name"); // 使用更标准的数据库名
        String tableName = params.getRequired("source.tablename");

        // Flink Job 参数
        int sinkParallelism = params.getInt("sink.parallelism", 4);
        long checkpointInterval = params.getLong("checkpoint.interval.ms", 5000L);


        // --- 2. Flink 环境设置 ---

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 推荐设置 Checkpoint，Iceberg Sink 依赖它来保证写入一致性
        env.enableCheckpointing(checkpointInterval);

        // 设置默认并行度
        env.setParallelism(params.getInt("job.parallelism", 1));


        // --- 3. 初始化 Iceberg Table 对象 ---

        // Flink Job 需要一个 Table 对象来配置 Sink
        Table targetTable = initializeIcebergTable(
                params.get(CATALOG_NAME, "glue_catalog"), // 默认使用 glue_catalog
                databaseName,
                tableName
        );

        // --- 4. Source 和 DataStream ---

        DataStream<SinkProto.RowRecord> pixelsRpcSource = env
                .addSource(new PixelsSource(serverHost, serverPort)) // 假设您的 Source 接受 host/port
                .name("pixelsRpcSource");

        DataStream<RowData> pixelsRowDataStream = pixelsRpcSource
                // 使用 ProtobufToRowDataMapFunction 进行转换
                .map(new ProtobufToRowDataMapFunction())
                .name("protobuf-to-rowdata-converter");

        // --- 5. Iceberg Sink ---

        FlinkSink.Builder sinkBuilder = FlinkSink.forRowData(pixelsRowDataStream)
                .table(targetTable)
                .overwrite(false); // 避免意外覆盖

        // 从 Iceberg Table Schema 中获取等值字段 (Equality Fields) 实现 Upsert
        // Iceberg 标识符字段（Identifier Fields）就是用于 Upsert 的键。
        Set<String> equalityFields = targetTable.schema().identifierFieldNames();

        if (equalityFields != null && !equalityFields.isEmpty()) {
            System.out.println("Enabling Upsert mode by reading equality fields from Table Schema: " + equalityFields);
            sinkBuilder.equalityFieldColumns((List<String>) equalityFields);
        } else {
            System.out.println("No identifier fields found in Iceberg Table schema. Sink will perform normal append/delete based on RowKind.");
        }


        // 附加到流环境并构建 Sink
        DataStreamSink<RowData> sink = sinkBuilder.build();

        // 在返回的 DataStreamSink 对象上设置名称
        sink.name("iceberg-sink-to-" + databaseName + "." + tableName);

        env.execute("Pixels Flink CDC Sink Job");
    }

    /**
     * 初始化 Iceberg Table 对象，使用 GlueCatalog 示例
     */
    private static Table initializeIcebergTable(
            String catalogName,
            String databaseName,
            String tableName) {


        GlueCatalog glueCatalog = new GlueCatalog();
        // 需要在此处配置 AWS 认证信息并调用 glueCatalog.initialize()，
        // 例如：glueCatalog.initialize(catalogName, properties);

        TableIdentifier tableIdentifier = TableIdentifier.of(databaseName, tableName);

        if (!glueCatalog.tableExists(tableIdentifier)) {
            throw new IllegalStateException("Iceberg Table not found: " + tableIdentifier);
        }

        return glueCatalog.loadTable(tableIdentifier);
    }
}