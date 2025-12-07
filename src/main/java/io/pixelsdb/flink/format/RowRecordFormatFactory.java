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
 import org.apache.flink.configuration.ConfigOption;
 import org.apache.flink.configuration.ReadableConfig;
 import org.apache.flink.table.connector.format.DecodingFormat;
 import org.apache.flink.table.data.RowData;
 import org.apache.flink.table.factories.DeserializationFormatFactory;
 import org.apache.flink.table.factories.DynamicTableFactory;
 import org.apache.flink.table.factories.FactoryUtil;

 import java.util.Collections;
 import java.util.Set;

 public class RowRecordFormatFactory implements DeserializationFormatFactory
 {
     private static final String IDENTIFIER = "pixels-rowrecord";

     @Override
     public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig readableConfig)
     {
         FactoryUtil.validateFactoryOptions(this, readableConfig);
         return new RowRecordFormat();
     }

     @Override
     public String factoryIdentifier()
     {
         return IDENTIFIER;
     }

     @Override
     public Set<ConfigOption<?>> requiredOptions()
     {
         return Collections.emptySet();
     }

     @Override
     public Set<ConfigOption<?>> optionalOptions()
     {
         return Collections.emptySet();
     }
 }
