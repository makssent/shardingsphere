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
import org.apache.shardingsphere.database.connector.firebird.metadata.data.FirebirdTypeMetadataRegistry;
import org.apache.shardingsphere.database.connector.firebird.metadata.data.FirebirdTypeMetadataRegistry.FieldType;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads physical Firebird field descriptors without JDBC character-set conversion.
 */
@RequiredArgsConstructor
final class FirebirdTypeMetadataLoader {
    
    private static final String FIELD_COLUMNS = "f.RDB$FIELD_TYPE, f.RDB$FIELD_SUB_TYPE, f.RDB$FIELD_SCALE, f.RDB$FIELD_LENGTH, f.RDB$CHARACTER_SET_ID, f.RDB$CHARACTER_LENGTH";
    
    private static final String COLUMN_SQL = "SELECT rf.RDB$FIELD_NAME, " + FIELD_COLUMNS
            + " FROM RDB$RELATION_FIELDS rf JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME WHERE rf.RDB$RELATION_NAME = ?";
    
    private static final String DOMAIN_SQL = "SELECT f.RDB$FIELD_NAME, " + FIELD_COLUMNS + " FROM RDB$FIELDS f WHERE COALESCE(f.RDB$SYSTEM_FLAG, 0) = 0";
    
    private final MetaDataLoaderMaterial material;
    
    void load() throws SQLException {
        try (MetaDataLoaderConnection connection = new MetaDataLoaderConnection(material.getStorageType(), material.getDataSource().getConnection())) {
            for (String each : material.getActualTableNames()) {
                try (PreparedStatement statement = connection.prepareStatement(COLUMN_SQL)) {
                    statement.setString(1, each);
                    FirebirdTypeMetadataRegistry.refreshTable(material.getDataSource(), each, readTypes(statement));
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(DOMAIN_SQL)) {
                FirebirdTypeMetadataRegistry.refreshDomains(material.getDataSource(), readTypes(statement));
            }
        }
    }
    
    private Map<String, FieldType> readTypes(final PreparedStatement statement) throws SQLException {
        Map<String, FieldType> result = new HashMap<>();
        try (ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                result.put(resultSet.getString(1).trim(), new FieldType(resultSet.getInt(2), resultSet.getInt(3), resultSet.getInt(4), resultSet.getInt(5), resultSet.getInt(6), resultSet.getInt(7)));
            }
        }
        return result;
    }
}
