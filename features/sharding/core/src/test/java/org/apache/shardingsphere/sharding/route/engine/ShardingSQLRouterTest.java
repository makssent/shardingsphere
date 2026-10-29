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

package org.apache.shardingsphere.sharding.route.engine;

import org.apache.shardingsphere.infra.binder.context.segment.insert.keygen.GeneratedKeyContext;
import org.apache.shardingsphere.infra.binder.context.statement.type.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.hint.HintValueContext;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.rule.RuleMetaData;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.apache.shardingsphere.infra.session.query.QueryContext;
import org.apache.shardingsphere.infra.util.props.PropertiesBuilder;
import org.apache.shardingsphere.sharding.route.engine.fixture.ShardingRouteEngineFixtureBuilder;
import org.apache.shardingsphere.sharding.rule.ShardingRule;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.InsertStatement;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ShardingSQLRouterTest {
    
    @Test
    void assertCreateRouteContext() {
        InsertStatementContext statementContext = mock(InsertStatementContext.class, RETURNS_DEEP_STUBS);
        when(statementContext.getSqlStatement()).thenReturn(mock(InsertStatement.class));
        when(statementContext.getInsertSelectContext()).thenReturn(null);
        when(statementContext.getTablesContext().getTableNames()).thenReturn(Collections.singleton("t_order"));
        GeneratedKeyContext generatedKeyContext = new GeneratedKeyContext("order_id", true);
        when(statementContext.getGeneratedKeyContext()).thenReturn(Optional.of(generatedKeyContext));
        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.getSqlStatementContext()).thenReturn(statementContext);
        when(queryContext.getHintValueContext()).thenReturn(new HintValueContext());
        when(queryContext.isExecutionPlan()).thenReturn(true);
        ShardingRule rule = ShardingRouteEngineFixtureBuilder.createBasedShardingRule();
        RouteContext actual = new ShardingSQLRouter().createRouteContext(queryContext, mock(RuleMetaData.class), mock(ShardingSphereDatabase.class),
                rule, Collections.singleton("t_order"), new ConfigurationProperties(PropertiesBuilder.build()));
        assertThat(actual.getRouteUnits().stream().map(each -> each.getDataSourceMapper().getActualName() + "." + each.getTableMappers().iterator().next().getActualName())
                .collect(Collectors.toList()), contains("ds_0.t_order_0", "ds_0.t_order_1", "ds_1.t_order_0", "ds_1.t_order_1"));
        assertTrue(actual.getOriginalDataNodes().isEmpty());
        assertTrue(generatedKeyContext.getGeneratedValues().isEmpty());
    }
}
