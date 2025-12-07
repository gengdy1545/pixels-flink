 /*
  * Copyright 2025 PixelsDB.
  *
  * This file is part of Pixels.
  *
  * Pixels is free software: you can redistribute it and/or modify
  * it under the terms of the Affero GNU General Public License as
  * published by the Free Software Foundation, either version 3 of
  * the License, or (at your option) any later version.
  *
  * Pixels is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  * Affero GNU General Public License for more details.
  *
  * You should have received a copy of the Affero GNU General Public
  * License along with Pixels.  If not, see
  * <https://www.gnu.org/licenses/>.
  */

 package io.pixelsdb.flink.format;

 import com.google.protobuf.ByteString;
 import io.pixelsdb.pixels.sink.SinkProto;
 import org.apache.flink.api.common.serialization.DeserializationSchema;
 import org.apache.flink.api.common.typeinfo.TypeInformation;
 import org.apache.flink.table.connector.RuntimeConverter.Context;
 import org.apache.flink.table.connector.source.DynamicTableSource;
 import org.apache.flink.table.data.GenericRowData;
 import org.apache.flink.table.data.RowData;
 import org.apache.flink.table.data.StringData;
 import org.apache.flink.table.types.logical.LogicalType;
 import org.apache.flink.table.types.logical.LogicalTypeRoot;
 import org.apache.flink.types.RowKind;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import java.io.IOException;
 import java.math.BigDecimal;
 import java.nio.ByteBuffer;
 import java.nio.ByteOrder;
 import java.nio.charset.StandardCharsets;
 import java.util.List;

 public class ChangelogRowRecordDeserializer implements DeserializationSchema<RowData>
 {
     private static final Logger LOGGER = LoggerFactory.getLogger(ChangelogRowRecordDeserializer.class);
     private final List<LogicalType> parsingTypes;
     private final DynamicTableSource.DataStructureConverter converter;
     private final TypeInformation<RowData> producedTypeInfo;

     public ChangelogRowRecordDeserializer(List<LogicalType> parsingTypes, DynamicTableSource.DataStructureConverter converter, TypeInformation<RowData> producedTypeInfo)
     {
         this.parsingTypes = parsingTypes;
         this.converter = converter;
         this.producedTypeInfo = producedTypeInfo;
     }

     @Override
     public void open(InitializationContext context) throws Exception
     {
         converter.open(Context.create(ChangelogRowRecordDeserializer.class.getClassLoader()));
     }

     @Override
     public RowData deserialize(byte[] bytes) throws IOException
     {
         SinkProto.RowRecord rowRecord = SinkProto.RowRecord.parseFrom(bytes);
         return deserialize(rowRecord);
     }

     public RowData deserialize(SinkProto.RowRecord rowRecord) throws IOException
     {
         SinkProto.OperationType op = rowRecord.getOp();

         switch (op)
         {
             case INSERT:
             case SNAPSHOT:
                 if (rowRecord.hasAfter())
                 {
                     return convert(rowRecord.getAfter(), RowKind.INSERT);
                 }
                 break;
             case UPDATE:
                 // In Flink DeserializationSchema, we typically only return the RowKind.UPDATE_AFTER
                 // and let the connector handle the UPDATE_BEFORE if necessary, but
                 // for a simple stream deserializer, we provide the full transaction (BEFORE then AFTER)
                 // or just the AFTER image. Here, we must only return one RowData.
                 // We prioritize the UPDATE_AFTER image.
                 if (rowRecord.hasAfter())
                 {
                     return convert(rowRecord.getAfter(), RowKind.UPDATE_AFTER);
                 }
                 if (rowRecord.hasBefore())
                 {
                     return convert(rowRecord.getBefore(), RowKind.UPDATE_BEFORE);
                 }
                 break;
             case DELETE:
                 if (rowRecord.hasBefore())
                 {
                     return convert(rowRecord.getBefore(), RowKind.DELETE);
                 }
                 break;
             default:
                 LOGGER.warn("Unknown operation type: {}", op);
         }

         // If no relevant row is found (e.g., UPDATE with neither before nor after), return null
         // or throw an exception, depending on error handling policy. Returning null skips the record.
         return null;
     }

     private RowData convert(SinkProto.RowValue rowValue, RowKind kind)
     {
         List<SinkProto.ColumnValue> values = rowValue.getValuesList();
         int arity = parsingTypes.size();
         GenericRowData row = new GenericRowData(kind, arity);

         // Assuming the order of values in RowValue matches the schema definition
         for (int i = 0; i < arity; i++)
         {
             if (i < values.size())
             {
                 ByteString byteString = values.get(i).getValue();
                 LogicalType type = parsingTypes.get(i);
                 row.setField(i, parseValue(byteString, type));
             } else
             {
                 // Missing value, set null
                 row.setField(i, null);
             }
         }

         // NOTE: The original map function was RichFlatMapFunction<...>, which suggests it could output multiple rows
         // (UPDATE_BEFORE, UPDATE_AFTER). A simple DeserializationSchema must return only one RowData.
         // For a full CDC stream, a specialized Flink DeserializationSchema (like Kafka's JSON Debezium format)
         // is needed to handle multi-row changes or use RowKind correctly.
         // For this simple rewrite, we return the final RowData image and its kind.

         return (RowData) converter.toInternal(row);
     }

     // --- Type Conversion Logic Moved from Original Class ---
     private Object parseValue(ByteString byteString, LogicalType type)
     {
         // If the ByteString is null or empty, return null for the field value.
         if (byteString == null || byteString.isEmpty())
         {
             return null;
         }

         byte[] bytes = byteString.toByteArray();
         ByteBuffer buffer = ByteBuffer.wrap(bytes);

         // Set to Big Endian (Network Byte Order)
         buffer.order(ByteOrder.BIG_ENDIAN);

         LogicalTypeRoot typeRoot = type.getTypeRoot();

         switch (typeRoot)
         {
             case CHAR:
             case VARCHAR:
             {
                 String value = byteString.toString(StandardCharsets.UTF_8);
                 return StringData.fromString(value);
             }

             case DECIMAL:
             {
                 // DECIMAL was serialized as a UTF-8 String representation.
                 String decimalString = byteString.toString(StandardCharsets.UTF_8);
                 // Note: The original code returned BigDecimal. For Flink RowData, we should
                 // ideally return DecimalData, but keeping BigDecimal for structural consistency
                 // if the converter handles it downstream.
                 return new BigDecimal(decimalString);
             }

             case BINARY:
             case VARBINARY:
             {
                 return byteString.toByteArray();
             }

             case INTEGER:
             case DATE:
             {
                 if (buffer.remaining() < Integer.BYTES)
                 {
                     throw new IllegalArgumentException("Invalid byte length for INT/DATE.");
                 }
                 return buffer.getInt();
             }

             case BIGINT:
             case TIME_WITHOUT_TIME_ZONE:
             case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
             case TIMESTAMP_WITH_TIME_ZONE:
             case TIMESTAMP_WITHOUT_TIME_ZONE:
             {
                 if (buffer.remaining() < Long.BYTES)
                 {
                     throw new IllegalArgumentException("Invalid byte length for BIGINT/TIMESTAMP.");
                 }
                 return buffer.getLong();
             }

             case FLOAT:
             {
                 if (buffer.remaining() < Float.BYTES)
                 {
                     throw new IllegalArgumentException("Invalid byte length for FLOAT.");
                 }
                 int intBits = buffer.getInt();
                 return Float.intBitsToFloat(intBits);
             }

             case DOUBLE:
             {
                 if (buffer.remaining() < Double.BYTES)
                 {
                     throw new IllegalArgumentException("Invalid byte length for DOUBLE.");
                 }
                 long longBits = buffer.getLong();
                 return Double.longBitsToDouble(longBits);
             }

             case BOOLEAN:
             {
                 String value = byteString.toStringUtf8();
                 return Boolean.parseBoolean(value);
             }

             default:
                 throw new UnsupportedOperationException("Unsupported type for deserialization: " + typeRoot);
         }
     }

     @Override
     public boolean isEndOfStream(RowData rowData)
     {
         return false;
     }

     @Override
     public TypeInformation<RowData> getProducedType()
     {
         return producedTypeInfo;
     }
 }
