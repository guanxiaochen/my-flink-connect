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

package org.apache.flink.connector.jdbc.dialect;

import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.converter.OracleRowConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * JDBC dialect for Oracle.
 */
public class OracleDialect extends AbstractDialect {

	private static final long serialVersionUID = 1L;

	// Define MAX/MIN precision of TIMESTAMP type:
	private static final int MAX_TIMESTAMP_PRECISION = 9;
	private static final int MIN_TIMESTAMP_PRECISION = 1;

	// Define MAX/MIN precision of DECIMAL type:
	private static final int MAX_DECIMAL_PRECISION = 38;
	private static final int MIN_DECIMAL_PRECISION = 1;

	@Override
	public boolean canHandle(String url) {
		return url.startsWith("jdbc:oracle:");
	}

	@Override
	public JdbcRowConverter getRowConverter(RowType rowType) {
		return new OracleRowConverter(rowType);
	}

	@Override
	public Optional<String> defaultDriverName() {
		return Optional.of("oracle.jdbc.OracleDriver");
	}

	@Override
	public String quoteIdentifier(String identifier) {
		return identifier;
	}


	/**
	 * Oracle upsert query. It use MERGE INTO to replace into Oracle.
	 */
	@Override
	public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
		String sourceFieldValues = Arrays.stream(fieldNames)
			.map(f -> "? " + quoteIdentifier(f))
			.collect(Collectors.joining(", "));
		String sourceSelect = "SELECT " + sourceFieldValues + " FROM DUAL";
		return Optional.of(getMergeIntoStatement(tableName, fieldNames, uniqueKeyFields, sourceSelect));
	}


	/**
	 * Get merge into statement (sourceSelect could be slightly different between vendor).
	 */
	String getMergeIntoStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields, String sourceSelect) {
		final Set<String> uniqueKeyFieldsSet = Arrays.stream(uniqueKeyFields).collect(Collectors.toSet());
		String onClause = Arrays.stream(uniqueKeyFields)
			.map(f -> "t." + quoteIdentifier(f) + "=s." + quoteIdentifier(f))
			.collect(Collectors.joining(", "));
		String updateClause = Arrays.stream(fieldNames)
			.filter(f -> !uniqueKeyFieldsSet.contains(f))
			.map(f -> "t." + quoteIdentifier(f) + "=s." + quoteIdentifier(f))
			.collect(Collectors.joining(", "));
		String insertValueClause = Arrays.stream(fieldNames)
			.map(f -> "s." + quoteIdentifier(f))
			.collect(Collectors.joining(", "));
		String columns = Arrays.stream(fieldNames)
			.map(this::quoteIdentifier)
			.collect(Collectors.joining(", "));
		// if we can't divide schema and table-name is risky to call quoteIdentifier(tableName)
		// for example in SQL-server [tbo].[sometable] is ok but [tbo.sometable] is not
		return "MERGE INTO " + tableName + " t " +
			"USING (" + sourceSelect + ") s " +
			"ON (" + onClause + ")" +
			" WHEN MATCHED THEN UPDATE SET " + updateClause +
			" WHEN NOT MATCHED THEN INSERT (" + columns + ") VALUES (" + insertValueClause + ")";
	}

	@Override
	public String dialectName() {
		return "Oracle";
	}

	@Override
	public int maxDecimalPrecision() {
		return MAX_DECIMAL_PRECISION;
	}

	@Override
	public int minDecimalPrecision() {
		return MIN_DECIMAL_PRECISION;
	}

	@Override
	public int maxTimestampPrecision() {
		return MAX_TIMESTAMP_PRECISION;
	}

	@Override
	public int minTimestampPrecision() {
		return MIN_TIMESTAMP_PRECISION;
	}

	@Override
	public List<LogicalTypeRoot> unsupportedTypes() {
		// TODO: We can't convert BINARY data type to
		//  PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in LegacyTypeInfoDataTypeConverter.
		return Arrays.asList(
			LogicalTypeRoot.BINARY,
			LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
			LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
			LogicalTypeRoot.INTERVAL_YEAR_MONTH,
			LogicalTypeRoot.INTERVAL_DAY_TIME,
			LogicalTypeRoot.MULTISET,
			LogicalTypeRoot.MAP,
			LogicalTypeRoot.ROW,
			LogicalTypeRoot.DISTINCT_TYPE,
			LogicalTypeRoot.STRUCTURED_TYPE,
			LogicalTypeRoot.NULL,
			LogicalTypeRoot.RAW,
			LogicalTypeRoot.SYMBOL,
			LogicalTypeRoot.UNRESOLVED
		);
	}

}
