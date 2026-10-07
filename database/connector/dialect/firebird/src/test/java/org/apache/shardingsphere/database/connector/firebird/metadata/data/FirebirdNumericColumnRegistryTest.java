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
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;

class FirebirdNumericColumnRegistryTest {
    
    private static final FirebirdNumericColumn DECIMAL_COLUMN = new FirebirdNumericColumn(16, 2, -4);
    
    @AfterEach
    void tearDown() {
        FirebirdNumericColumnRegistry.refreshTable("schema_a", "table_a", Collections.emptyMap());
    }
    
    @Test
    void assertFindNumericColumnIgnoresCase() {
        FirebirdNumericColumnRegistry.refreshTable("schema_a", "table_a", Collections.singletonMap("amount", DECIMAL_COLUMN));
        assertThat(FirebirdNumericColumnRegistry.findNumericColumn("SCHEMA_A", "TABLE_A", "AMOUNT"), is(Optional.of(DECIMAL_COLUMN)));
    }
    
    @Test
    void assertFindNumericColumnByActualTableName() {
        FirebirdNumericColumnRegistry.refreshTable("schema_a", "table_a", Collections.singletonMap("amount", DECIMAL_COLUMN));
        assertFalse(FirebirdNumericColumnRegistry.findNumericColumn("schema_a", "table", "amount").isPresent());
    }
    
    @Test
    void assertRefreshTableRemovesEntryWithEmptyColumns() {
        FirebirdNumericColumnRegistry.refreshTable("schema_a", "table_a", Collections.singletonMap("amount", DECIMAL_COLUMN));
        FirebirdNumericColumnRegistry.refreshTable("schema_a", "table_a", Collections.emptyMap());
        assertFalse(FirebirdNumericColumnRegistry.findNumericColumn("schema_a", "table_a", "amount").isPresent());
    }
    
    @Test
    void assertRefreshTableSkipsNullEntries() {
        Map<String, FirebirdNumericColumn> numericColumns = new HashMap<>(2, 1F);
        numericColumns.put(null, DECIMAL_COLUMN);
        numericColumns.put("amount", null);
        FirebirdNumericColumnRegistry.refreshTable("schema_a", "table_a", numericColumns);
        assertFalse(FirebirdNumericColumnRegistry.findNumericColumn("schema_a", "table_a", "amount").isPresent());
    }
    
    @Test
    void assertFindNumericColumnWithNullNames() {
        FirebirdNumericColumnRegistry.refreshTable(null, null, Collections.singletonMap("amount", DECIMAL_COLUMN));
        assertFalse(FirebirdNumericColumnRegistry.findNumericColumn("schema_a", null, "amount").isPresent());
        assertFalse(FirebirdNumericColumnRegistry.findNumericColumn("schema_a", "table_a", null).isPresent());
    }
}
