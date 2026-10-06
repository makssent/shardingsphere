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
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for Firebird blob columns.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class FirebirdBlobInfoRegistry {
    
    private static final Map<String, Map<String, FirebirdBlobColumn>> BLOB_COLUMNS = new ConcurrentHashMap<>();
    
    /**
     * Refresh blob column metadata for a table.
     *
     * @param schemaName schema name
     * @param tableName table name
     * @param blobColumns blob column name to blob column mapping
     */
    public static void refreshTable(final String schemaName, final String tableName, final Map<String, FirebirdBlobColumn> blobColumns) {
        if (null == tableName) {
            return;
        }
        String tableKey = buildTableKey(schemaName, tableName);
        if (null == blobColumns || blobColumns.isEmpty()) {
            BLOB_COLUMNS.remove(tableKey);
            return;
        }
        Map<String, FirebirdBlobColumn> normalizedColumns = new HashMap<>(blobColumns.size(), 1F);
        for (Entry<String, FirebirdBlobColumn> entry : blobColumns.entrySet()) {
            if (null != entry.getKey()) {
                normalizedColumns.put(toKey(entry.getKey()), entry.getValue());
            }
        }
        if (normalizedColumns.isEmpty()) {
            BLOB_COLUMNS.remove(tableKey);
            return;
        }
        BLOB_COLUMNS.put(tableKey, Collections.unmodifiableMap(normalizedColumns));
    }
    
    /**
     * Determine whether column is a Firebird blob column.
     *
     * @param schemaName schema name
     * @param tableName table name
     * @param columnName column name
     * @return whether column is a blob column
     */
    public static boolean isBlobColumn(final String schemaName, final String tableName, final String columnName) {
        if (null == tableName || null == columnName) {
            return false;
        }
        Map<String, FirebirdBlobColumn> blobColumns = BLOB_COLUMNS.get(buildTableKey(schemaName, tableName));
        return null != blobColumns && blobColumns.containsKey(toKey(columnName));
    }
    
    /**
     * Find blob subtype for a column.
     *
     * @param schemaName schema name
     * @param tableName table name
     * @param columnName column name
     * @return blob subtype if present
     */
    public static OptionalInt findBlobSubtype(final String schemaName, final String tableName, final String columnName) {
        FirebirdBlobColumn blobColumn = findBlobColumn(schemaName, tableName, columnName);
        return null == blobColumn || null == blobColumn.getSubtype() ? OptionalInt.empty() : OptionalInt.of(blobColumn.getSubtype());
    }
    
    /**
     * Find character set ID of a blob column.
     *
     * @param schemaName schema name
     * @param tableName table name
     * @param columnName column name
     * @return character set ID if blob column is present
     */
    public static OptionalInt findBlobCharsetId(final String schemaName, final String tableName, final String columnName) {
        FirebirdBlobColumn blobColumn = findBlobColumn(schemaName, tableName, columnName);
        return null == blobColumn ? OptionalInt.empty() : OptionalInt.of(blobColumn.getCharsetId());
    }
    
    private static FirebirdBlobColumn findBlobColumn(final String schemaName, final String tableName, final String columnName) {
        if (null == tableName || null == columnName) {
            return null;
        }
        Map<String, FirebirdBlobColumn> blobColumns = BLOB_COLUMNS.get(buildTableKey(schemaName, tableName));
        return null == blobColumns ? null : blobColumns.get(toKey(columnName));
    }
    
    private static String buildTableKey(final String schemaName, final String tableName) {
        String schemaKey = null == schemaName ? "" : toKey(schemaName);
        String logicTable = trimToLogicTableName(tableName);
        return schemaKey + "." + toKey(logicTable);
    }
    
    private static String trimToLogicTableName(final String tableName) {
        int end = tableName.length() - 1;
        while (end >= 0 && !Character.isLetter(tableName.charAt(end))) {
            end--;
        }
        return end < 0 ? tableName : tableName.substring(0, end + 1);
    }
    
    private static String toKey(final String value) {
        return value.toUpperCase(Locale.ENGLISH);
    }
}
