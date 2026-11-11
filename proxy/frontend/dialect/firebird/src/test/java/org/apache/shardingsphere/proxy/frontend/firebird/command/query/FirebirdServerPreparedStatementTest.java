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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query;

import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.hint.HintValueContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

class FirebirdServerPreparedStatementTest {
    
    @Test
    void assertGetPhysicalParameterMarkersReusesParsedStatement() {
        FirebirdServerPreparedStatement statement = new FirebirdServerPreparedStatement("foo_sql", mock(SQLStatementContext.class), new HintValueContext());
        AtomicInteger calls = new AtomicInteger();
        Function<String, List<Integer>> parser = sql -> {
            calls.incrementAndGet();
            return Collections.singletonList(sql.length());
        };
        List<Integer> first = statement.getPhysicalParameterMarkers("foo_sql", 2, parser);
        List<Integer> actual = statement.getPhysicalParameterMarkers("foo_sql", 2, parser);
        assertThat(actual, sameInstance(first));
        assertThat(calls.get(), is(1));
    }
    
    @Test
    void assertGetPhysicalParameterMarkersEvictsLeastRecentlyUsedStatement() {
        FirebirdServerPreparedStatement statement = new FirebirdServerPreparedStatement("foo_sql", mock(SQLStatementContext.class), new HintValueContext());
        AtomicInteger calls = new AtomicInteger();
        Function<String, List<Integer>> parser = sql -> {
            calls.incrementAndGet();
            return Collections.singletonList(sql.length());
        };
        statement.getPhysicalParameterMarkers("foo_sql", 2, parser);
        statement.getPhysicalParameterMarkers("bar_sql", 2, parser);
        statement.getPhysicalParameterMarkers("foo_sql", 2, parser);
        statement.getPhysicalParameterMarkers("baz_sql", 2, parser);
        statement.getPhysicalParameterMarkers("bar_sql", 2, parser);
        assertThat(calls.get(), is(4));
    }
    
    @Test
    void assertGetPhysicalParameterMarkersWithDisabledCache() {
        FirebirdServerPreparedStatement statement = new FirebirdServerPreparedStatement("foo_sql", mock(SQLStatementContext.class), new HintValueContext());
        AtomicInteger calls = new AtomicInteger();
        Function<String, List<Integer>> parser = sql -> {
            calls.incrementAndGet();
            return Collections.singletonList(sql.length());
        };
        statement.getPhysicalParameterMarkers("foo_sql", 1, parser);
        statement.getPhysicalParameterMarkers("foo_sql", 0, parser);
        statement.getPhysicalParameterMarkers("foo_sql", 1, parser);
        assertThat(calls.get(), is(3));
    }
}
