/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for Phoenix.
 */
public class PhoenixRowConverter extends AbstractJdbcRowConverter {

	private static final long serialVersionUID = 1L;

	@Override
	public String converterName() {
		return "Phoenix";
	}

	public PhoenixRowConverter(RowType rowType) {
		super(rowType);
	}

	@Override
	protected JdbcDeserializationConverter createInternalConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case FLOAT:
				return val -> ((Number) val).floatValue();
			case DOUBLE:
				return val -> ((Number) val).doubleValue();
			case TINYINT:
				return val -> ((Number) val).byteValue();
			case SMALLINT:
				return val -> ((Number) val).shortValue();
			case INTEGER:
				return val -> ((Number) val).intValue();
			case BIGINT:
				return val -> ((Number) val).longValue();
			case DATE:
				return val -> {
					if (val instanceof Timestamp) {
						return (int) (((Timestamp) val).toLocalDateTime().toLocalDate().toEpochDay());
					}
					return (int) (((Date) val).toLocalDate().toEpochDay());
				};
			case TIME_WITHOUT_TIME_ZONE:
				return val -> {
					if (val instanceof Timestamp) {
						return (int) (((Timestamp) val).toLocalDateTime().toLocalTime().toNanoOfDay()
							/ 1_000_000L);
					}
					return (int) (((Time) val).toLocalTime().toNanoOfDay() / 1_000_000L);
				};
			case TIMESTAMP_WITH_TIME_ZONE:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return val -> {
					if (val instanceof Date) {
						return TimestampData.fromInstant(((Date) val).toInstant());
					}
					return TimestampData.fromTimestamp((Timestamp) val);
				};
		}
		return super.createInternalConverter(type);
	}
}
