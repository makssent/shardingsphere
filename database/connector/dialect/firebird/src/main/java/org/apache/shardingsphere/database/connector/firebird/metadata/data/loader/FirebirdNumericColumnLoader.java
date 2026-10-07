/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.database.connector.firebird.metadata.data.loader;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.database.connector.core.metadata.data.loader.MetaDataLoaderConnection;
import org.apache.shardingsphere.database.connector.core.metadata.data.loader.MetaDataLoaderMaterial;
import org.apache.shardingsphere.database.connector.firebird.metadata.data.FirebirdNumericColumn;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Loader for Firebird numeric columns.
 *
 * <p>Loads NUMERIC and DECIMAL columns stored as SMALLINT, INTEGER or BIGINT ({@code RDB$FIELD_TYPE} 7, 8 and 16) with their sub type and scale.</p>
 */
@RequiredArgsConstructor
final class FirebirdNumericColumnLoader {
    
    private static final String SELECT_NUMERIC_COLUMNS_SQL =
            "SELECT TRIM(rf.RDB$FIELD_NAME) AS COLUMN_NAME, f.RDB$FIELD_TYPE AS FIELD_TYPE, f.RDB$FIELD_SUB_TYPE AS SUB_TYPE, f.RDB$FIELD_SCALE AS FIELD_SCALE "
                    + "FROM RDB$RELATION_FIELDS rf "
                    + "JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME "
                    + "WHERE TRIM(UPPER(rf.RDB$RELATION_NAME)) = ? "
                    + "AND f.RDB$FIELD_TYPE IN (7, 8, 16) AND (f.RDB$FIELD_SUB_TYPE IN (1, 2) OR f.RDB$FIELD_SCALE < 0)";
    
    private final MetaDataLoaderMaterial material;
    
    Map<String, Map<String, FirebirdNumericColumn>> load() throws SQLException {
        if (material.getActualTableNames().isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Map<String, FirebirdNumericColumn>> result = new HashMap<>(material.getActualTableNames().size(), 1F);
        try (MetaDataLoaderConnection connection = new MetaDataLoaderConnection(material.getStorageType(), material.getDataSource().getConnection())) {
            for (String each : material.getActualTableNames()) {
                result.put(each, loadTableNumericColumns(connection, each));
            }
        }
        return result;
    }
    
    private Map<String, FirebirdNumericColumn> loadTableNumericColumns(final MetaDataLoaderConnection connection, final String formattedTableName) throws SQLException {
        Map<String, FirebirdNumericColumn> result = new HashMap<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_NUMERIC_COLUMNS_SQL)) {
            preparedStatement.setString(1, formattedTableName.toUpperCase(Locale.ENGLISH));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    if (null != columnName && !columnName.trim().isEmpty()) {
                        FirebirdNumericColumn numericColumn = new FirebirdNumericColumn(resultSet.getInt("FIELD_TYPE"), resultSet.getInt("SUB_TYPE"), resultSet.getInt("FIELD_SCALE"));
                        result.put(columnName.trim().toUpperCase(Locale.ENGLISH), numericColumn);
                    }
                }
            }
        }
        return result.isEmpty() ? Collections.emptyMap() : Collections.unmodifiableMap(result);
    }
}
