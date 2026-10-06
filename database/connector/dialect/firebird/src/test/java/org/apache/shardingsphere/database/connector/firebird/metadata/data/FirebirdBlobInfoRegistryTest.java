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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.internal.configuration.plugins.Plugins;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FirebirdBlobInfoRegistryTest {
    
    private Map<String, Map<String, FirebirdBlobColumn>> blobColumns;
    
    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws ReflectiveOperationException {
        blobColumns = (Map<String, Map<String, FirebirdBlobColumn>>) Plugins.getMemberAccessor().get(FirebirdBlobInfoRegistry.class.getDeclaredField("BLOB_COLUMNS"), FirebirdBlobInfoRegistry.class);
        blobColumns.clear();
    }
    
    @AfterEach
    void tearDown() {
        blobColumns.clear();
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("refreshTableRemoveCases")
    void assertRefreshTableRemovesEntry(final String name, final Map<String, FirebirdBlobColumn> newColumns) {
        blobColumns.put("SCHEMA_A.TABLE_A", Collections.singletonMap("EXISTING_COL", createBlobColumn(1)));
        FirebirdBlobInfoRegistry.refreshTable("schema_a", "table_a", newColumns);
        assertFalse(blobColumns.containsKey("SCHEMA_A.TABLE_A"));
    }
    
    @Test
    void assertRefreshTableWhenTableNameIsNull() {
        registerBlobColumns("SENTINEL", Collections.singletonMap("EXISTING_COL", createBlobColumn(9)));
        FirebirdBlobInfoRegistry.refreshTable("schema_a", null, Collections.singletonMap("blob_col", createBlobColumn(1)));
        assertTrue(blobColumns.containsKey("SENTINEL"));
        assertThat(blobColumns.get("SENTINEL").get("EXISTING_COL").getSubtype(), is(9));
    }
    
    @Test
    void assertRefreshTable() {
        Map<String, FirebirdBlobColumn> newColumns = createColumnsWithMixedNames();
        FirebirdBlobInfoRegistry.refreshTable(null, "table_1", newColumns);
        Map<String, FirebirdBlobColumn> actual = blobColumns.get(".TABLE");
        assertTrue(actual.containsKey("BLOB_COL"));
        assertThat(actual.get("BLOB_COL").getSubtype(), is(2));
        assertThat(actual.size(), is(1));
        assertThrows(UnsupportedOperationException.class, () -> actual.put("ANOTHER_COL", createBlobColumn(3)));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("notBlobColumnCases")
    void assertIsBlobColumnWhenNotMatched(final String name, final String tableName, final String columnName, final boolean registerTable, final boolean registerColumn) {
        if (registerTable) {
            blobColumns.put("SCHEMA_A.TABLE_A", registerColumn ? Collections.singletonMap("BLOB_COL", createBlobColumn(1)) : Collections.singletonMap("OTHER_COL", createBlobColumn(1)));
        }
        assertFalse(FirebirdBlobInfoRegistry.isBlobColumn("schema_a", tableName, columnName));
    }
    
    @Test
    void assertIsBlobColumn() {
        registerBlobColumns("SCHEMA_A.123", Collections.singletonMap("BLOB_COL", createBlobColumn(1)));
        boolean actual = FirebirdBlobInfoRegistry.isBlobColumn("schema_a", "123", "blob_col");
        assertTrue(actual);
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("findBlobSubtypeEmptyCases")
    void assertFindBlobSubtypeWhenAbsent(final String name, final String tableName, final String columnName, final boolean registerTable, final boolean registerColumn) {
        if (registerTable) {
            blobColumns.put("SCHEMA_A.TABLE_A", registerColumn ? Collections.singletonMap("BLOB_COL", createBlobColumn(5)) : Collections.singletonMap("OTHER_COL", createBlobColumn(5)));
        }
        OptionalInt actual = FirebirdBlobInfoRegistry.findBlobSubtype("schema_a", tableName, columnName);
        assertFalse(actual.isPresent());
    }
    
    @Test
    void assertFindBlobSubtype() {
        registerBlobColumns("SCHEMA_A.TABLE_A", Collections.singletonMap("BLOB_COL", createBlobColumn(7)));
        OptionalInt actual = FirebirdBlobInfoRegistry.findBlobSubtype("schema_a", "table_a", "blob_col");
        assertTrue(actual.isPresent());
        assertThat(actual.getAsInt(), is(7));
    }
    
    @Test
    void assertFindBlobCharsetId() {
        registerBlobColumns("SCHEMA_A.TABLE_A", Collections.singletonMap("BLOB_COL", new FirebirdBlobColumn(1, 4)));
        OptionalInt actual = FirebirdBlobInfoRegistry.findBlobCharsetId("schema_a", "table_a", "blob_col");
        assertTrue(actual.isPresent());
        assertThat(actual.getAsInt(), is(4));
    }
    
    @Test
    void assertFindBlobCharsetIdWhenColumnAbsent() {
        registerBlobColumns("SCHEMA_A.TABLE_A", Collections.singletonMap("OTHER_COL", new FirebirdBlobColumn(1, 4)));
        assertFalse(FirebirdBlobInfoRegistry.findBlobCharsetId("schema_a", "table_a", "blob_col").isPresent());
    }
    
    private static Stream<Arguments> refreshTableRemoveCases() {
        return Stream.of(
                Arguments.of("null_blob_columns", null),
                Arguments.of("empty_blob_columns", Collections.emptyMap()),
                Arguments.of("all_column_names_are_null", Collections.singletonMap(null, createBlobColumn(1))));
    }
    
    private static Stream<Arguments> notBlobColumnCases() {
        return Stream.of(
                Arguments.of("null_table_name", null, "blob_col", false, false),
                Arguments.of("null_column_name", "table_a", null, true, true),
                Arguments.of("table_not_registered", "table_a", "blob_col", false, false),
                Arguments.of("column_not_registered", "table_a", "blob_col", true, false));
    }
    
    private static Stream<Arguments> findBlobSubtypeEmptyCases() {
        return Stream.of(
                Arguments.of("null_table_name", null, "blob_col", false, false),
                Arguments.of("null_column_name", "table_a", null, true, true),
                Arguments.of("table_not_registered", "table_a", "blob_col", false, false),
                Arguments.of("column_not_registered", "table_a", "blob_col", true, false));
    }
    
    private static Map<String, FirebirdBlobColumn> createColumnsWithMixedNames() {
        Map<String, FirebirdBlobColumn> result = new HashMap<>(2, 1F);
        result.put("blob_col", createBlobColumn(2));
        result.put(null, createBlobColumn(9));
        return result;
    }
    
    private static FirebirdBlobColumn createBlobColumn(final int subtype) {
        return new FirebirdBlobColumn(subtype, 0);
    }
    
    private void registerBlobColumns(final String tableKey, final Map<String, FirebirdBlobColumn> columns) {
        blobColumns.put(tableKey, columns);
    }
}
