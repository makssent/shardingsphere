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

import com.google.common.collect.MapMaker;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Firebird physical column and domain types, owned by the data source that supplied them.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class FirebirdTypeMetadataRegistry {
    
    private static final Map<DataSource, Map<String, Map<String, FieldType>>> COLUMN_TYPES = new MapMaker().weakKeys().makeMap();
    
    private static final Map<DataSource, Map<String, FieldType>> DOMAIN_TYPES = new MapMaker().weakKeys().makeMap();
    
    /**
     * Refresh types for one physical table.
     *
     * @param dataSource physical data source
     * @param tableName physical table name
     * @param columnTypes column types indexed by their exact database identifiers
     */
    public static void refreshTable(final DataSource dataSource, final String tableName, final Map<String, FieldType> columnTypes) {
        COLUMN_TYPES.computeIfAbsent(dataSource, key -> new ConcurrentHashMap<>()).put(tableName, columnTypes);
    }
    
    /**
     * Find a physical column type.
     *
     * @param dataSource physical data source
     * @param tableName physical table name
     * @param columnName physical column name
     * @return column type
     */
    @HighFrequencyInvocation
    public static Optional<FieldType> findColumnType(final DataSource dataSource, final String tableName, final String columnName) {
        Map<String, Map<String, FieldType>> tables = COLUMN_TYPES.getOrDefault(dataSource, Collections.emptyMap());
        Map<String, FieldType> columns = tables.getOrDefault(tableName, tables.getOrDefault(tableName.toUpperCase(Locale.ENGLISH), Collections.emptyMap()));
        return Optional.ofNullable(columns.getOrDefault(columnName, columns.get(columnName.toUpperCase(Locale.ENGLISH))));
    }
    
    /**
     * Refresh user-defined domains.
     *
     * @param dataSource physical data source
     * @param domainTypes types indexed by their exact database identifiers
     */
    public static void refreshDomains(final DataSource dataSource, final Map<String, FieldType> domainTypes) {
        DOMAIN_TYPES.put(dataSource, domainTypes);
    }
    
    /**
     * Find a domain type.
     *
     * @param dataSource physical data source
     * @param domainName domain identifier as written in SQL
     * @return domain type
     */
    @HighFrequencyInvocation
    public static Optional<FieldType> findDomainType(final DataSource dataSource, final String domainName) {
        String identifier = domainName.startsWith("\"") ? domainName.substring(1, domainName.length() - 1).replace("\"\"", "\"") : domainName.toUpperCase(Locale.ENGLISH);
        return Optional.ofNullable(DOMAIN_TYPES.getOrDefault(dataSource, Collections.emptyMap()).get(identifier));
    }
    
    /**
     * A Firebird catalog field descriptor, independent of JDBC connection character-set negotiation.
     */
    @RequiredArgsConstructor
    @Getter
    public static final class FieldType {
        
        private final int type;
        
        private final int subType;
        
        private final int scale;
        
        private final int length;
        
        private final int characterSetId;
        
        private final int characterLength;
    }
}
