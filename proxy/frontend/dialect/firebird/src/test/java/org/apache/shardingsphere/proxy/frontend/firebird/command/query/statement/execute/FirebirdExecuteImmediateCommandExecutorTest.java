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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query.statement.execute;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.DefaultAttributeMap;
import org.apache.shardingsphere.database.connector.core.spi.DatabaseTypedSPILoader;
import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.ExcessTransactionsException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidTransactionHandleException;
import org.apache.shardingsphere.database.protocol.constant.CommonConstants;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.FirebirdCommandPacketType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.execute.FirebirdExecuteImmediatePacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.generic.FirebirdGenericResponsePacket;
import org.apache.shardingsphere.database.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.hint.HintValueContext;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.rule.RuleMetaData;
import org.apache.shardingsphere.infra.metadata.user.Grantee;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.handler.ProxyBackendHandler;
import org.apache.shardingsphere.proxy.backend.handler.ProxyBackendHandlerFactory;
import org.apache.shardingsphere.proxy.backend.handler.ProxySQLComQueryParser;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.command.CommandExecutorTask;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.transaction.FirebirdTransactionIdGenerator;
import org.apache.shardingsphere.proxy.frontend.spi.DatabaseProtocolFrontendEngine;
import org.apache.shardingsphere.sql.parser.statement.core.enums.TransactionIsolationLevel;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.InsertStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.tcl.CommitStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.tcl.SetTransactionStatement;
import org.apache.shardingsphere.test.infra.framework.extension.mock.AutoMockExtension;
import org.apache.shardingsphere.test.infra.framework.extension.mock.StaticMockSettings;
import org.apache.shardingsphere.transaction.api.TransactionType;
import org.apache.shardingsphere.transaction.rule.TransactionRule;
import org.firebirdsql.gds.ISCConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(AutoMockExtension.class)
@StaticMockSettings({ProxySQLComQueryParser.class, ProxyBackendHandlerFactory.class, ProxyContext.class})
@MockitoSettings(strictness = Strictness.LENIENT)
class FirebirdExecuteImmediateCommandExecutorTest {
    
    private static final DatabaseType DATABASE_TYPE = TypedSPILoader.getService(DatabaseType.class, "Firebird");
    
    private static final int CONNECTION_ID = 3;
    
    private static final String INSERT_SQL = "INSERT INTO T VALUES (1)";
    
    @Mock
    private FirebirdExecuteImmediatePacket packet;
    
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConnectionSession connectionSession;
    
    @Mock
    private ProxyBackendHandler proxyBackendHandler;
    
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ChannelHandlerContext channelHandlerContext;
    
    @Mock
    private Connection backendConnection;
    
    @BeforeEach
    void setUp() {
        FirebirdTransactionIdGenerator.getInstance().registerConnection(CONNECTION_ID);
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
    }
    
    @AfterEach
    void tearDown() {
        FirebirdTransactionIdGenerator.getInstance().unregisterConnection(CONNECTION_ID);
    }
    
    @Test
    void assertExecuteSetTransaction() throws SQLException {
        when(ProxySQLComQueryParser.parse("SET TRANSACTION", DATABASE_TYPE, connectionSession)).thenReturn(new SetTransactionStatement(DATABASE_TYPE));
        when(packet.getSQL()).thenReturn("SET TRANSACTION");
        Collection<DatabasePacket> actual = new FirebirdExecuteImmediateCommandExecutor(packet, connectionSession).execute();
        assertThat(((FirebirdGenericResponsePacket) actual.iterator().next()).getHandle(), is(1));
        assertTrue(FirebirdTransactionIdGenerator.getInstance().isTransactionActive(CONNECTION_ID, 1));
        verify(connectionSession).setAutoCommit(false);
        verify(connectionSession).setReadOnly(false);
        verify(connectionSession).setIsolationLevel(TransactionIsolationLevel.REPEATABLE_READ);
    }
    
    @Test
    void assertExecuteSetTransactionInBackendTransaction() throws SQLException {
        when(ProxySQLComQueryParser.parse("SET TRANSACTION", DATABASE_TYPE, connectionSession)).thenReturn(new SetTransactionStatement(DATABASE_TYPE));
        when(packet.getSQL()).thenReturn("SET TRANSACTION");
        when(connectionSession.getTransactionStatus().isInTransaction()).thenReturn(true);
        new FirebirdExecuteImmediateCommandExecutor(packet, connectionSession).execute();
        verify(connectionSession, never()).setAutoCommit(anyBoolean());
        verify(connectionSession, never()).setReadOnly(anyBoolean());
        verify(connectionSession, never()).setIsolationLevel(any());
    }
    
    @Test
    void assertExecuteWithoutTransaction() {
        when(ProxySQLComQueryParser.parse(INSERT_SQL, DATABASE_TYPE, connectionSession)).thenReturn(InsertStatement.builder().databaseType(DATABASE_TYPE).build());
        when(packet.getSQL()).thenReturn(INSERT_SQL);
        assertThrows(InvalidTransactionHandleException.class, () -> new FirebirdExecuteImmediateCommandExecutor(packet, connectionSession).execute());
    }
    
    @Test
    void assertExecuteInTransaction() throws SQLException {
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        InsertStatement insertStatement = InsertStatement.builder().databaseType(DATABASE_TYPE).build();
        when(ProxySQLComQueryParser.parse(INSERT_SQL, DATABASE_TYPE, connectionSession)).thenReturn(insertStatement);
        when(ProxyBackendHandlerFactory.newInstance(eq(DATABASE_TYPE), eq(INSERT_SQL), eq(insertStatement), eq(connectionSession), any(HintValueContext.class))).thenReturn(proxyBackendHandler);
        when(packet.getSQL()).thenReturn(INSERT_SQL);
        when(packet.getTransactionId()).thenReturn(transactionId);
        when(packet.getHintValueContext()).thenReturn(new HintValueContext());
        when(connectionSession.getTransactionStatus().isInTransaction()).thenReturn(true);
        Collection<DatabasePacket> actual = new FirebirdExecuteImmediateCommandExecutor(packet, connectionSession).execute();
        assertThat(((FirebirdGenericResponsePacket) actual.iterator().next()).getHandle(), is(transactionId));
        verify(proxyBackendHandler).execute();
        int anotherTransactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        assertThrows(ExcessTransactionsException.class, () -> FirebirdTransactionIdGenerator.getInstance().checkTransactionForStatement(connectionSession, anotherTransactionId));
    }
    
    @Test
    void assertExecuteCommitInTransaction() throws SQLException {
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        CommitStatement commitStatement = new CommitStatement(DATABASE_TYPE);
        when(ProxySQLComQueryParser.parse("COMMIT", DATABASE_TYPE, connectionSession)).thenReturn(commitStatement);
        when(ProxyBackendHandlerFactory.newInstance(eq(DATABASE_TYPE), eq("COMMIT"), eq(commitStatement), eq(connectionSession), any(HintValueContext.class))).thenReturn(proxyBackendHandler);
        when(packet.getSQL()).thenReturn("COMMIT");
        when(packet.getTransactionId()).thenReturn(transactionId);
        when(packet.getHintValueContext()).thenReturn(new HintValueContext());
        Collection<DatabasePacket> actual = new FirebirdExecuteImmediateCommandExecutor(packet, connectionSession).execute();
        assertThat(((FirebirdGenericResponsePacket) actual.iterator().next()).getHandle(), is(0));
        assertFalse(FirebirdTransactionIdGenerator.getInstance().isTransactionActive(CONNECTION_ID, transactionId));
    }
    
    @Test
    void assertExecuteWithoutTransactionKeepsTransactionWork() throws SQLException {
        ConnectionSession transactionSession = createConnectionSessionInTransaction();
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        FirebirdTransactionIdGenerator.getInstance().updateBackendTransactionOwner(transactionSession, transactionId);
        when(ProxySQLComQueryParser.parse(INSERT_SQL, DATABASE_TYPE, transactionSession)).thenReturn(InsertStatement.builder().databaseType(DATABASE_TYPE).build());
        DatabaseProtocolFrontendEngine engine = DatabaseTypedSPILoader.getService(DatabaseProtocolFrontendEngine.class, DATABASE_TYPE);
        new CommandExecutorTask(engine, transactionSession, channelHandlerContext, Unpooled.buffer().writeInt(FirebirdCommandPacketType.EXEC_IMMEDIATE.getValue()).writeInt(0).writeInt(0).writeInt(3)
                .writeInt(INSERT_SQL.length()).writeBytes(INSERT_SQL.getBytes(StandardCharsets.US_ASCII)).writeInt(0).writeInt(0)).run();
        new CommandExecutorTask(engine, transactionSession, channelHandlerContext, Unpooled.buffer().writeInt(FirebirdCommandPacketType.COMMIT.getValue()).writeInt(transactionId)).run();
        ArgumentCaptor<FirebirdGenericResponsePacket> responseCaptor = ArgumentCaptor.forClass(FirebirdGenericResponsePacket.class);
        verify(channelHandlerContext, times(2)).write(responseCaptor.capture());
        assertThat(responseCaptor.getAllValues().get(0).getErrorCode(), is(ISCConstants.isc_bad_trans_handle));
        verify(backendConnection).commit();
        verify(backendConnection, never()).rollback();
    }
    
    private ConnectionSession createConnectionSessionInTransaction() {
        TransactionRule transactionRule = mock(TransactionRule.class);
        when(transactionRule.getDefaultType()).thenReturn(TransactionType.LOCAL);
        when(ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData()).thenReturn(
                new ShardingSphereMetaData(Collections.emptyList(), mock(), new RuleMetaData(Collections.singleton(transactionRule)), new ConfigurationProperties(new Properties())));
        when(channelHandlerContext.channel().attr(CommonConstants.CHARSET_ATTRIBUTE_KEY).get()).thenReturn(StandardCharsets.UTF_8);
        ConnectionSession result = new ConnectionSession(DATABASE_TYPE, new DefaultAttributeMap());
        result.setConnectionId(CONNECTION_ID);
        result.setGrantee(new Grantee("foo_user", "%"));
        result.setAutoCommit(false);
        result.getDatabaseConnectionManager().handleAutoCommit();
        result.getDatabaseConnectionManager().getCachedConnections().put("foo_db.foo_ds", backendConnection);
        return result;
    }
}
