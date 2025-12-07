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

 import org.apache.flink.api.common.serialization.DeserializationSchema;
 import org.apache.flink.api.common.typeinfo.TypeInformation;
 import org.apache.flink.table.connector.ChangelogMode;
 import org.apache.flink.table.connector.format.DecodingFormat;
 import org.apache.flink.table.connector.source.DynamicTableSource;
 import org.apache.flink.table.data.RowData;
 import org.apache.flink.table.types.DataType;
 import org.apache.flink.table.types.logical.LogicalType;
 import org.apache.flink.types.RowKind;

 import java.util.List;

 public class RowRecordFormat implements DecodingFormat<DeserializationSchema<RowData>>
 {

     @Override
     public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType producedDataType)
     {
         final TypeInformation<RowData> producedTypeInfo = context.createTypeInformation(producedDataType);
         final DynamicTableSource.DataStructureConverter converter = context.createDataStructureConverter(producedDataType);
         final List<LogicalType> parsingTypes = producedDataType.getLogicalType().getChildren();
         return new ChangelogRowRecordDeserializer(parsingTypes, converter, producedTypeInfo);
     }

     @Override
     public ChangelogMode getChangelogMode()
     {
         return ChangelogMode.newBuilder()
                 .addContainedKind(RowKind.DELETE)
                 .addContainedKind(RowKind.INSERT)
                 .addContainedKind(RowKind.UPDATE_BEFORE)
                 .addContainedKind(RowKind.UPDATE_AFTER)
                 .build();
     }
 }
