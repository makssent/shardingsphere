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

package org.apache.shardingsphere.database.connector.firebird.metadata.data;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for Firebird numeric columns.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class FirebirdNumericColumnRegistry {
    
    private static final Map<String, Map<String, FirebirdNumericColumn>> NUMERIC_COLUMNS = new ConcurrentHashMap<>();
    
    /**
     * Refresh numeric column metadata for a table.
     *
     * @param schemaName schema name
     * @param tableName actual table name
     * @param numericColumns numeric column name to numeric column mapping
     */
    public static void refreshTable(final String schemaName, final String tableName, final Map<String, FirebirdNumericColumn> numericColumns) {
        if (null == tableName) {
            return;
        }
        String tableKey = buildTableKey(schemaName, tableName);
        if (null == numericColumns || numericColumns.isEmpty()) {
            NUMERIC_COLUMNS.remove(tableKey);
            return;
        }
        Map<String, FirebirdNumericColumn> normalizedColumns = new HashMap<>(numericColumns.size(), 1F);
        for (Entry<String, FirebirdNumericColumn> entry : numericColumns.entrySet()) {
            if (null != entry.getKey() && null != entry.getValue()) {
                normalizedColumns.put(toKey(entry.getKey()), entry.getValue());
            }
        }
        if (normalizedColumns.isEmpty()) {
            NUMERIC_COLUMNS.remove(tableKey);
            return;
        }
        NUMERIC_COLUMNS.put(tableKey, Collections.unmodifiableMap(normalizedColumns));
    }
    
    /**
     * Find numeric column.
     *
     * @param schemaName schema name
     * @param tableName actual table name
     * @param columnName column name
     * @return numeric column if present
     */
    public static Optional<FirebirdNumericColumn> findNumericColumn(final String schemaName, final String tableName, final String columnName) {
        if (null == tableName || null == columnName) {
            return Optional.empty();
        }
        Map<String, FirebirdNumericColumn> numericColumns = NUMERIC_COLUMNS.get(buildTableKey(schemaName, tableName));
        return null == numericColumns ? Optional.empty() : Optional.ofNullable(numericColumns.get(toKey(columnName)));
    }
    
    private static String buildTableKey(final String schemaName, final String tableName) {
        String schemaKey = null == schemaName ? "" : toKey(schemaName);
        return schemaKey + "." + toKey(tableName);
    }
    
    private static String toKey(final String value) {
        return value.toUpperCase(Locale.ENGLISH);
    }
}
