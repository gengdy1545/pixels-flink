## Pixels Flink RPC Source Connector (pixels-flink)

This project, **`pixels-flink`**, is an Apache Flink **Dynamic Table Connector** designed to act as a **Source**. It allows Flink to subscribe to and consume **RPC Stream Table** data published by the pixels-sink Flink Writer.

The connector is built to read data in **ChangeLog** format, enabling seamless integration with Flink's Change Data Capture (CDC) stream processing workflows.

## Getting Started

This project is deployed as a Flink plugin. It must be built as a JAR file and placed in Flink's **`plugins`** directory.

### 1\. Building the Project

Use Maven to build the project. The **`maven-shade-plugin`** must be correctly configured to merge service files (especially for gRPC dependencies) to ensure proper loading in the Flink plugin environment.

```bash
mvn clean package
```

### 2\. Deploying to Flink

Move the generated JAR file (e.g., `pixels-flink-0.1.jar`) to a dedicated directory within Flink's plugins folder.

```bash
# Create a dedicated plugin directory
mkdir -p $FLINK_HOME/plugins/pixels-sink-connector
# Copy the JAR file
cp target/pixels-flink-0.1.jar $FLINK_HOME/plugins/pixels-sink-connector/
```

### 3\. Flink SQL DDL Example

In the Flink SQL Client, use the following Data Definition Language (DDL) to define a table that subscribes to the PixelsDB data stream.

```sql
-- Ensure Flink Session Cluster is running: ./bin/start-cluster.sh
$FLINK_HOME/bin/sql-client.sh

Flink SQL> CREATE TABLE pixels_loanapps (
-- Schema must strictly match the PixelsDB table
      id             INT,
      applicantid    INT,
      amount         FLOAT,
      duration       INT,
      status         CHAR(12),
      ts             TIMESTAMP(6),
      freshness_ts   TIMESTAMP(3)  
) WITH (
      -- Connector and Format Configuration
      'connector' = 'pixels-sink',
      'format' = 'pixels-rowrecord',
      
      -- PixelsDB RPC Connection Properties
      'pixels.host' = 'realtime-pixels-coordinator', -- Hostname or IP of the Coordinator
      'pixels.port' = '9091',                        -- RPC Port
      'pixels.database' = 'pixels_bench_sf10x',      -- PixelsDB Database (Schema) Name
      'pixels.table' = 'loanapps'                    -- Table Name to subscribe to
);
```

### 4\. Querying the Stream

Once the table is defined, you can query it like any other Flink streaming source:

```sql
-- Subscribe to and view the continuous stream of data
Flink SQL> SELECT * FROM pixels_loanapps;
```

-----

## Configuration Options

| Option Name | Default Value | Required | Description |
| :--- | :--- | :--- | :--- |
| **`connector`** | N/A | pixels-sink | Must be set to `'pixels-sink'`. |
| **`format`** | N/A | pixels-rowrecord | Must be set to `'pixels-rowrecord'`. This format handles the CDC ChangeLog. |
| **`pixels.host`** | N/A | Yes | The hostname or IP address of the PixelsDB Coordinator. |
| **`pixels.port`** | 9091 | Yes | The RPC port of the PixelsDB Coordinator (e.g., `9091`). |
| **`pixels.database`** | N/A | Yes | The target database (schema) name in PixelsDB. |
| **`pixels.table`** | N/A | Yes | The specific table name to subscribe to within PixelsDB. |

-----

## Debugging and Testing

The project includes JUnit 5 test cases to verify connector functionality in a local development environment:

  * `io.pixelsdb.flink.TestPixelsRpcClient.java`: Tests gRPC client connectivity.
  * `io.pixelsdb.flink.TestPixelsRpcSourceTableApi.java`: Provides an end-to-end test for registering and reading the source via the Flink Table API.