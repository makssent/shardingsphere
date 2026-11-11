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

import org.apache.shardingsphere.database.connector.firebird.metadata.data.FirebirdTypeMetadataRegistry.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

class FirebirdTypeMetadataRegistryTest {
    
    private final DataSource dataSource = mock(DataSource.class);
    
    private final DataSource otherDataSource = mock(DataSource.class);
    
    @AfterEach
    void tearDown() {
        FirebirdTypeMetadataRegistry.refreshTable(dataSource, "foo_0", Collections.emptyMap());
        FirebirdTypeMetadataRegistry.refreshTable(dataSource, "foo_1", Collections.emptyMap());
        FirebirdTypeMetadataRegistry.refreshTable(otherDataSource, "foo_0", Collections.emptyMap());
        FirebirdTypeMetadataRegistry.refreshDomains(dataSource, Collections.emptyMap());
    }
    
    @Test
    void assertFindColumnTypeKeepsPhysicalSourcesSeparate() {
        FieldType first = new FieldType(37, 0, 0, 10, 0, 10);
        FieldType second = new FieldType(37, 0, 0, 40, 4, 10);
        FirebirdTypeMetadataRegistry.refreshTable(dataSource, "foo_0", Collections.singletonMap("COL", first));
        FirebirdTypeMetadataRegistry.refreshTable(dataSource, "foo_1", Collections.singletonMap("COL", second));
        FirebirdTypeMetadataRegistry.refreshTable(otherDataSource, "foo_0", Collections.singletonMap("COL", second));
        assertThat(FirebirdTypeMetadataRegistry.findColumnType(dataSource, "foo_0", "COL").get(), sameInstance(first));
        assertThat(FirebirdTypeMetadataRegistry.findColumnType(dataSource, "foo_1", "COL").get(), sameInstance(second));
        assertThat(FirebirdTypeMetadataRegistry.findColumnType(otherDataSource, "foo_0", "COL").get(), sameInstance(second));
    }
    
    @Test
    void assertRefreshTableReplacesRemovedColumn() {
        FirebirdTypeMetadataRegistry.refreshTable(dataSource, "foo_0", Collections.singletonMap("COL", new FieldType(16, 1, -2, 8, 0, 0)));
        FirebirdTypeMetadataRegistry.refreshTable(dataSource, "foo_0", Collections.emptyMap());
        assertFalse(FirebirdTypeMetadataRegistry.findColumnType(dataSource, "foo_0", "COL").isPresent());
        assertFalse(FirebirdTypeMetadataRegistry.findColumnType(otherDataSource, "foo_1", "COL").isPresent());
    }
    
    @Test
    void assertFindDomainTypePreservesQuotedIdentifiers() {
        FieldType upper = new FieldType(16, 1, -2, 8, 0, 0);
        FieldType quoted = new FieldType(37, 0, 0, 10, 0, 10);
        Map<String, FieldType> types = new HashMap<>(2, 1F);
        types.put("FOO_DOMAIN", upper);
        types.put("foo_domain", quoted);
        FirebirdTypeMetadataRegistry.refreshDomains(dataSource, types);
        assertThat(FirebirdTypeMetadataRegistry.findDomainType(dataSource, "foo_domain").get(), sameInstance(upper));
        assertThat(FirebirdTypeMetadataRegistry.findDomainType(dataSource, "\"foo_domain\"").get(), sameInstance(quoted));
        assertFalse(FirebirdTypeMetadataRegistry.findDomainType(otherDataSource, "foo_domain").isPresent());
    }
}
