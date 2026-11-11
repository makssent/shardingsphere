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

import org.apache.shardingsphere.database.connector.core.metadata.data.loader.DialectMetaDataLoader;
import org.apache.shardingsphere.database.connector.core.metadata.data.loader.MetaDataLoaderMaterial;
import org.apache.shardingsphere.database.connector.core.metadata.data.loader.type.TableMetaDataLoader;
import org.apache.shardingsphere.database.connector.core.metadata.data.model.SchemaMetaData;
import org.apache.shardingsphere.database.connector.core.metadata.data.model.TableMetaData;
import org.apache.shardingsphere.database.connector.core.spi.DatabaseTypedSPILoader;
import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.database.connector.firebird.metadata.data.FirebirdBlobInfoRegistry;
import org.apache.shardingsphere.database.connector.firebird.metadata.data.FirebirdNonFixedLengthColumnSizeRegistry;
import org.apache.shardingsphere.database.connector.firebird.metadata.data.FirebirdTypeMetadataRegistry;
import org.apache.shardingsphere.database.connector.firebird.metadata.data.FirebirdTypeMetadataRegistry.FieldType;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FirebirdMetaDataLoaderTest {
    
    private final DatabaseType databaseType = TypedSPILoader.getService(DatabaseType.class, "Firebird");
    
    private final DialectMetaDataLoader dialectMetaDataLoader = DatabaseTypedSPILoader.getService(DialectMetaDataLoader.class, databaseType);
    
    @Test
    void assertLoadRefreshesSizeRegistry() throws SQLException {
        DataSource dataSource = mock(DataSource.class);
        MetaDataLoaderMaterial material = new MetaDataLoaderMaterial(Collections.singleton("TEST_TABLE"), "logic_ds", dataSource, databaseType, "schema");
        TableMetaData tableMetaData = new TableMetaData("TEST_TABLE", Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        Map<String, Integer> tableSizes = Collections.singletonMap("COLUMN", 16);
        Map<String, Map<String, Integer>> allSizes = Collections.singletonMap("TEST_TABLE", tableSizes);
        Map<String, Integer> tableBlobColumns = Collections.singletonMap("BLOB_COL", 1);
        Map<String, Map<String, Integer>> allBlobColumns = Collections.singletonMap("TEST_TABLE", tableBlobColumns);
        try (
                MockedStatic<TableMetaDataLoader> tableLoaderMocked = mockStatic(TableMetaDataLoader.class);
                MockedStatic<FirebirdNonFixedLengthColumnSizeRegistry> sizeRegistryMocked = mockStatic(FirebirdNonFixedLengthColumnSizeRegistry.class);
                MockedStatic<FirebirdBlobInfoRegistry> blobRegistryMocked = mockStatic(FirebirdBlobInfoRegistry.class);
                MockedConstruction<FirebirdNonFixedLengthColumnSizeLoader> columnSizeLoaderMocked =
                        mockConstruction(FirebirdNonFixedLengthColumnSizeLoader.class, (mock, context) -> when(mock.load()).thenReturn(allSizes));
                MockedConstruction<FirebirdTypeMetadataLoader> typeLoaderMocked = mockConstruction(FirebirdTypeMetadataLoader.class);
                MockedConstruction<FirebirdBlobColumnLoader> blobColumnLoaderMocked =
                        mockConstruction(FirebirdBlobColumnLoader.class, (mock, context) -> when(mock.load()).thenReturn(allBlobColumns))) {
            tableLoaderMocked.when(() -> TableMetaDataLoader.loadNormalized(dataSource, "TEST_TABLE", databaseType)).thenReturn(Optional.of(tableMetaData));
            Collection<SchemaMetaData> actual = dialectMetaDataLoader.load(material);
            assertThat(actual.size(), is(1));
            SchemaMetaData schema = actual.iterator().next();
            assertThat(schema.getName(), is("schema"));
            assertThat(schema.getTables(), contains(tableMetaData));
            sizeRegistryMocked.verify(() -> FirebirdNonFixedLengthColumnSizeRegistry.refreshTable("schema", "TEST_TABLE", tableSizes));
            blobRegistryMocked.verify(() -> FirebirdBlobInfoRegistry.refreshTable("schema", "TEST_TABLE", tableBlobColumns));
            verify(columnSizeLoaderMocked.constructed().get(0)).load();
            verify(blobColumnLoaderMocked.constructed().get(0)).load();
            verify(typeLoaderMocked.constructed().get(0)).load();
        }
    }
    
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"FOO", "CaseOnly", "foo\"Bar"})
    @SuppressWarnings("unchecked")
    void assertLoadPhysicalTypes(final String tableName) throws SQLException {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false, true, false);
        when(resultSet.getString(1)).thenReturn("COL   ", "FOO_DOMAIN   ");
        when(resultSet.getInt(2)).thenReturn(37, 16);
        when(resultSet.getInt(3)).thenReturn(0, 1);
        when(resultSet.getInt(4)).thenReturn(0, -2);
        when(resultSet.getInt(5)).thenReturn(10, 8);
        when(resultSet.getInt(6)).thenReturn(0);
        when(resultSet.getInt(7)).thenReturn(10, 0);
        try (
                MockedStatic<TableMetaDataLoader> tableLoaderMocked = mockStatic(TableMetaDataLoader.class);
                MockedStatic<FirebirdNonFixedLengthColumnSizeRegistry> sizeRegistryMocked = mockStatic(FirebirdNonFixedLengthColumnSizeRegistry.class);
                MockedStatic<FirebirdBlobInfoRegistry> blobRegistryMocked = mockStatic(FirebirdBlobInfoRegistry.class);
                MockedStatic<FirebirdTypeMetadataRegistry> registry = mockStatic(FirebirdTypeMetadataRegistry.class);
                MockedConstruction<FirebirdNonFixedLengthColumnSizeLoader> sizeLoaderMocked = mockConstruction(FirebirdNonFixedLengthColumnSizeLoader.class);
                MockedConstruction<FirebirdBlobColumnLoader> blobLoaderMocked = mockConstruction(FirebirdBlobColumnLoader.class)) {
            dialectMetaDataLoader.load(new MetaDataLoaderMaterial(Collections.singleton(tableName), "foo_ds", dataSource, databaseType, "foo_db"));
            ArgumentCaptor<Map<String, FieldType>> actualColumns = ArgumentCaptor.forClass(Map.class);
            registry.verify(() -> FirebirdTypeMetadataRegistry.refreshTable(eq(dataSource), eq(tableName), actualColumns.capture()));
            assertThat(actualColumns.getValue().get("COL").getCharacterSetId(), is(0));
            assertThat(actualColumns.getValue().get("COL").getLength(), is(10));
            ArgumentCaptor<Map<String, FieldType>> actualDomains = ArgumentCaptor.forClass(Map.class);
            registry.verify(() -> FirebirdTypeMetadataRegistry.refreshDomains(eq(dataSource), actualDomains.capture()));
            assertThat(actualDomains.getValue().get("FOO_DOMAIN").getType(), is(16));
            assertThat(actualDomains.getValue().get("FOO_DOMAIN").getSubType(), is(1));
            assertThat(actualDomains.getValue().get("FOO_DOMAIN").getScale(), is(-2));
        }
        verify(statement).setString(1, tableName);
        verify(connection).close();
    }
}
