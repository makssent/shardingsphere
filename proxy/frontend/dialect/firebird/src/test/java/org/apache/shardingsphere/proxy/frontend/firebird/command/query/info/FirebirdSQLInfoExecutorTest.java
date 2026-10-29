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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query.info;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.DefaultAttributeMap;
import org.apache.shardingsphere.database.connector.core.spi.DatabaseTypedSPILoader;
import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.database.protocol.constant.CommonConstants;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.FirebirdCommandPacketType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.FirebirdInfoPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.type.common.FirebirdCommonInfoPacketType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.type.sql.FirebirdSQLInfoPacketType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.type.sql.FirebirdSQLInfoReturnPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.generic.FirebirdGenericResponsePacket;
import org.apache.shardingsphere.database.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.engine.SQLBindEngine;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.connection.kernel.KernelProcessor;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionContext;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.context.SQLUnit;
import org.apache.shardingsphere.infra.hint.HintValueContext;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.rule.RuleMetaData;
import org.apache.shardingsphere.infra.metadata.user.Grantee;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.apache.shardingsphere.infra.session.query.QueryContext;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.infra.util.props.PropertiesBuilder;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.backend.session.ServerPreparedStatementRegistry;
import org.apache.shardingsphere.proxy.frontend.command.CommandExecutorTask;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.FirebirdServerPreparedStatement;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.transaction.FirebirdTransactionIdGenerator;
import org.apache.shardingsphere.proxy.frontend.spi.DatabaseProtocolFrontendEngine;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.DeleteStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.InsertStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.UpdateStatement;
import org.apache.shardingsphere.test.infra.framework.extension.mock.AutoMockExtension;
import org.apache.shardingsphere.test.infra.framework.extension.mock.StaticMockSettings;
import org.apache.shardingsphere.transaction.api.TransactionType;
import org.apache.shardingsphere.transaction.rule.TransactionRule;
import org.firebirdsql.gds.ISCConstants;
import org.firebirdsql.jdbc.FirebirdPreparedStatement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(AutoMockExtension.class)
@StaticMockSettings(ProxyContext.class)
class FirebirdSQLInfoExecutorTest {
    
    @Mock
    private FirebirdInfoPacket packet;
    
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConnectionSession connectionSession;
    
    @Test
    void assertExecute() {
        when(connectionSession.getServerPreparedStatementRegistry()).thenReturn(new ServerPreparedStatementRegistry());
        FirebirdSQLInfoExecutor executor = new FirebirdSQLInfoExecutor(packet, connectionSession);
        Collection<DatabasePacket> actual = executor.execute();
        assertThat(actual.iterator().next(), isA(FirebirdGenericResponsePacket.class));
        FirebirdGenericResponsePacket responsePacket = (FirebirdGenericResponsePacket) actual.iterator().next();
        assertThat(responsePacket.getData(), isA(FirebirdSQLInfoReturnPacket.class));
    }
    
    @Test
    void assertExecuteWithExecutionPlans() throws SQLException {
        when(packet.getHandle()).thenReturn(1);
        when(packet.getInfoItems()).thenReturn(Collections.singletonList(FirebirdSQLInfoPacketType.GET_PLAN));
        when(packet.getMaxLength()).thenReturn(1024);
        SQLStatementContext sqlContext = mock(SQLStatementContext.class, Answers.RETURNS_DEEP_STUBS);
        SQLStatement sqlStatement = mock(SQLStatement.class);
        when(sqlContext.getSqlStatement()).thenReturn(sqlStatement);
        when(sqlContext.getTablesContext().getDatabaseNames()).thenReturn(Collections.emptyList());
        ServerPreparedStatementRegistry registry = new ServerPreparedStatementRegistry();
        registry.addPreparedStatement(1, new FirebirdServerPreparedStatement("SELECT * FROM t WHERE id = ?", sqlContext, new HintValueContext()));
        when(connectionSession.getServerPreparedStatementRegistry()).thenReturn(registry);
        when(connectionSession.getCurrentDatabaseName()).thenReturn("foo_db");
        when(connectionSession.getConnectionContext().getCurrentDatabaseName()).thenReturn(Optional.of("foo_db"));
        ContextManager contextManager = mock(ContextManager.class, Answers.RETURNS_DEEP_STUBS);
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        when(contextManager.getMetaDataContexts().getMetaData().getDatabase("foo_db").getName()).thenReturn("foo_db");
        Connection connection = mock(Connection.class);
        when(connectionSession.getDatabaseConnectionManager().getConnections(any(), any(), anyInt(), anyInt(), any())).thenReturn(Collections.singletonList(connection));
        PreparedStatement first = mock(PreparedStatement.class);
        PreparedStatement second = mock(PreparedStatement.class);
        PreparedStatement third = mock(PreparedStatement.class);
        FirebirdPreparedStatement firstFirebird = mock(FirebirdPreparedStatement.class);
        FirebirdPreparedStatement secondFirebird = mock(FirebirdPreparedStatement.class);
        FirebirdPreparedStatement thirdFirebird = mock(FirebirdPreparedStatement.class);
        when(connection.prepareStatement("SELECT * FROM t_0 WHERE id = ?")).thenReturn(first);
        when(connection.prepareStatement("SELECT * FROM t_1 WHERE id = ?")).thenReturn(second);
        when(connection.prepareStatement("SELECT * FROM t_2 WHERE id = ?")).thenReturn(third);
        when(first.unwrap(FirebirdPreparedStatement.class)).thenReturn(firstFirebird);
        when(second.unwrap(FirebirdPreparedStatement.class)).thenReturn(secondFirebird);
        when(third.unwrap(FirebirdPreparedStatement.class)).thenReturn(thirdFirebird);
        when(firstFirebird.getExecutionPlan()).thenReturn("PLAN (T_0 NATURAL)");
        when(secondFirebird.getExecutionPlan()).thenReturn("PLAN (T_1 NATURAL)");
        when(thirdFirebird.getExecutionPlan()).thenReturn("");
        ExecutionUnit firstUnit = new ExecutionUnit("ds_0", new SQLUnit("SELECT * FROM t_0 WHERE id = ?", Collections.emptyList()));
        ExecutionUnit secondUnit = new ExecutionUnit("ds_1", new SQLUnit("SELECT * FROM t_1 WHERE id = ?", Collections.emptyList()));
        ExecutionUnit thirdUnit = new ExecutionUnit("ds_2", new SQLUnit("SELECT * FROM t_2 WHERE id = ?", Collections.emptyList()));
        try (
                MockedConstruction<SQLBindEngine> ignoredBind = mockConstruction(SQLBindEngine.class, (mock, context) -> when(mock.bind(any())).thenReturn(sqlContext));
                MockedConstruction<KernelProcessor> ignoredKernel = mockConstruction(KernelProcessor.class,
                        (mock, context) -> when(mock.generateExecutionContext(any(), any(), any()))
                                .thenReturn(new ExecutionContext(mock(QueryContext.class), Arrays.asList(firstUnit, secondUnit, thirdUnit), mock(RouteContext.class))))) {
            FirebirdGenericResponsePacket actual = (FirebirdGenericResponsePacket) new FirebirdSQLInfoExecutor(packet, connectionSession).execute().iterator().next();
            assertThat(((FirebirdSQLInfoReturnPacket) actual.getData()).getExecutionPlan(), is("PLAN (T_0 NATURAL)\nPLAN (T_1 NATURAL)"));
        }
        verify(first).close();
        verify(second).close();
        verify(third).close();
    }
    
    @Test
    void assertExecuteWithInvalidStatementHandleForPlan() {
        when(packet.getHandle()).thenReturn(1);
        when(packet.getInfoItems()).thenReturn(Collections.singletonList(FirebirdSQLInfoPacketType.GET_PLAN));
        when(connectionSession.getServerPreparedStatementRegistry()).thenReturn(new ServerPreparedStatementRegistry());
        FirebirdGenericResponsePacket actual = (FirebirdGenericResponsePacket) new FirebirdSQLInfoExecutor(packet, connectionSession).execute().iterator().next();
        assertThat(actual.getStatusVector().getSegments().size(), is(1));
        assertThat(actual.getStatusVector().getSegments().get(0).getGdsCode(), is(ISCConstants.isc_bad_req_handle));
        assertTrue(actual.getStatusVector().getSegments().get(0).getArguments().isEmpty());
        assertNull(actual.getStatusVector().getSegments().get(0).getSqlState());
    }
    
    @Test
    void assertExecuteWithPlanAfterEndItem() {
        when(packet.getInfoItems()).thenReturn(Arrays.asList(FirebirdCommonInfoPacketType.END, FirebirdSQLInfoPacketType.GET_PLAN));
        when(connectionSession.getServerPreparedStatementRegistry()).thenReturn(new ServerPreparedStatementRegistry());
        FirebirdGenericResponsePacket actual = (FirebirdGenericResponsePacket) new FirebirdSQLInfoExecutor(packet, connectionSession).execute().iterator().next();
        assertThat(((FirebirdSQLInfoReturnPacket) actual.getData()).getExecutionPlan(), is(""));
    }
    
    @Test
    void assertExecuteWithExecutionPlanBackendFailure() throws SQLException {
        when(packet.getHandle()).thenReturn(1);
        when(packet.getInfoItems()).thenReturn(Collections.singletonList(FirebirdSQLInfoPacketType.GET_PLAN));
        SQLStatementContext sqlContext = mock(SQLStatementContext.class, Answers.RETURNS_DEEP_STUBS);
        when(sqlContext.getSqlStatement()).thenReturn(mock(SQLStatement.class));
        when(sqlContext.getTablesContext().getDatabaseNames()).thenReturn(Collections.emptyList());
        ServerPreparedStatementRegistry registry = new ServerPreparedStatementRegistry();
        registry.addPreparedStatement(1, new FirebirdServerPreparedStatement("SELECT * FROM t", sqlContext, new HintValueContext()));
        when(connectionSession.getServerPreparedStatementRegistry()).thenReturn(registry);
        when(connectionSession.getCurrentDatabaseName()).thenReturn("foo_db");
        when(connectionSession.getConnectionContext().getCurrentDatabaseName()).thenReturn(Optional.of("foo_db"));
        ContextManager contextManager = mock(ContextManager.class, Answers.RETURNS_DEEP_STUBS);
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        when(contextManager.getMetaDataContexts().getMetaData().getDatabase("foo_db").getName()).thenReturn("foo_db");
        Connection connection = mock(Connection.class);
        when(connectionSession.getDatabaseConnectionManager().getConnections(any(), any(), anyInt(), anyInt(), any())).thenReturn(Collections.singletonList(connection));
        when(connection.prepareStatement("SELECT * FROM t")).thenThrow(new SQLException("backend failure", "HY000", ISCConstants.isc_bad_stmt_handle));
        ExecutionUnit unit = new ExecutionUnit("ds_0", new SQLUnit("SELECT * FROM t", Collections.emptyList()));
        try (
                MockedConstruction<SQLBindEngine> ignoredBind = mockConstruction(SQLBindEngine.class, (mock, context) -> when(mock.bind(any())).thenReturn(sqlContext));
                MockedConstruction<KernelProcessor> ignoredKernel = mockConstruction(KernelProcessor.class,
                        (mock, context) -> when(mock.generateExecutionContext(any(), any(), any()))
                                .thenReturn(new ExecutionContext(mock(QueryContext.class), Collections.singletonList(unit), mock(RouteContext.class))))) {
            FirebirdGenericResponsePacket actual = (FirebirdGenericResponsePacket) new FirebirdSQLInfoExecutor(packet, connectionSession).execute().iterator().next();
            assertThat(actual.getStatusVector().getSegments().size(), is(1));
            assertThat(actual.getStatusVector().getSegments().get(0).getGdsCode(), is(ISCConstants.isc_bad_stmt_handle));
            assertThat(actual.getStatusVector().getSegments().get(0).getArguments(), is(Collections.singletonList("backend failure")));
            assertNull(actual.getStatusVector().getSegments().get(0).getSqlState());
        }
    }
    
    @Test
    void assertInvalidStatementHandleForPlanInTransaction() throws SQLException {
        TransactionRule transactionRule = mock(TransactionRule.class);
        when(transactionRule.getDefaultType()).thenReturn(TransactionType.LOCAL);
        when(ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData()).thenReturn(
                new ShardingSphereMetaData(Collections.emptyList(), mock(), new RuleMetaData(Collections.singleton(transactionRule)), new ConfigurationProperties(PropertiesBuilder.build())));
        ChannelHandlerContext context = mock(ChannelHandlerContext.class, Answers.RETURNS_DEEP_STUBS);
        when(context.channel().attr(CommonConstants.CHARSET_ATTRIBUTE_KEY).get()).thenReturn(StandardCharsets.UTF_8);
        Connection backend = mock(Connection.class);
        ConnectionSession session = new ConnectionSession(TypedSPILoader.getService(DatabaseType.class, "Firebird"), new DefaultAttributeMap());
        session.setConnectionId(42);
        session.setGrantee(new Grantee("foo_user", "%"));
        session.setAutoCommit(false);
        session.getDatabaseConnectionManager().handleAutoCommit();
        session.getDatabaseConnectionManager().getCachedConnections().put("foo_db.foo_ds", backend);
        FirebirdTransactionIdGenerator.getInstance().registerConnection(42);
        try {
            int transactionHandle = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(42);
            DatabaseProtocolFrontendEngine engine = DatabaseTypedSPILoader.getService(DatabaseProtocolFrontendEngine.class, TypedSPILoader.getService(DatabaseType.class, "Firebird"));
            new CommandExecutorTask(engine, session, context, Unpooled.buffer().writeInt(FirebirdCommandPacketType.INFO_SQL.getValue()).writeInt(1).writeInt(0)
                    .writeInt(2).writeByte(FirebirdSQLInfoPacketType.GET_PLAN.getCode()).writeByte(1).writeZero(2).writeInt(1024)).run();
            new CommandExecutorTask(engine, session, context, Unpooled.buffer().writeInt(FirebirdCommandPacketType.COMMIT.getValue()).writeInt(transactionHandle)).run();
            ArgumentCaptor<FirebirdGenericResponsePacket> responses = ArgumentCaptor.forClass(FirebirdGenericResponsePacket.class);
            verify(context, times(2)).write(responses.capture());
            assertThat(responses.getAllValues().get(0).getStatusVector().getSegments().size(), is(1));
            assertThat(responses.getAllValues().get(0).getStatusVector().getSegments().get(0).getGdsCode(), is(ISCConstants.isc_bad_req_handle));
            assertTrue(responses.getAllValues().get(0).getStatusVector().getSegments().get(0).getArguments().isEmpty());
            assertNull(responses.getAllValues().get(0).getStatusVector().getSegments().get(0).getSqlState());
            verify(backend).commit();
            verify(backend, never()).rollback();
        } finally {
            FirebirdTransactionIdGenerator.getInstance().unregisterConnection(42);
        }
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("sqlStatementProvider")
    void assertExecuteWithRecordsInfo(final String name, final SQLStatement sqlStatement, final long expectedInsertCount, final long expectedUpdateCount, final long expectedDeleteCount) {
        when(packet.getHandle()).thenReturn(1);
        ServerPreparedStatementRegistry registry = new ServerPreparedStatementRegistry();
        SQLStatementContext sqlStatementContext = mock(SQLStatementContext.class);
        when(sqlStatementContext.getSqlStatement()).thenReturn(sqlStatement);
        FirebirdServerPreparedStatement preparedStatement = new FirebirdServerPreparedStatement("DML", sqlStatementContext, new HintValueContext());
        preparedStatement.setAffectedRows(3L);
        registry.addPreparedStatement(1, preparedStatement);
        when(connectionSession.getServerPreparedStatementRegistry()).thenReturn(registry);
        FirebirdSQLInfoExecutor executor = new FirebirdSQLInfoExecutor(packet, connectionSession);
        Collection<DatabasePacket> actual = executor.execute();
        FirebirdGenericResponsePacket responsePacket = (FirebirdGenericResponsePacket) actual.iterator().next();
        FirebirdSQLInfoReturnPacket returnPacket = (FirebirdSQLInfoReturnPacket) responsePacket.getData();
        assertThat(returnPacket.getRecordsInfo().getInsertCount(), is(expectedInsertCount));
        assertThat(returnPacket.getRecordsInfo().getUpdateCount(), is(expectedUpdateCount));
        assertThat(returnPacket.getRecordsInfo().getDeleteCount(), is(expectedDeleteCount));
    }
    
    private static Stream<Arguments> sqlStatementProvider() {
        return Stream.of(
                Arguments.of("insert", mock(InsertStatement.class), 3L, 0L, 0L),
                Arguments.of("update", mock(UpdateStatement.class), 0L, 3L, 0L),
                Arguments.of("delete", mock(DeleteStatement.class), 0L, 0L, 3L));
    }
}
