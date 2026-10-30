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

package org.apache.shardingsphere.proxy.frontend.firebird;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.DefaultAttributeMap;
import org.apache.shardingsphere.database.connector.core.spi.DatabaseTypedSPILoader;
import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.database.exception.core.exception.syntax.sql.DialectSQLParsingException;
import org.apache.shardingsphere.database.exception.core.exception.transaction.InTransactionException;
import org.apache.shardingsphere.database.protocol.constant.CommonConstants;
import org.apache.shardingsphere.database.protocol.firebird.codec.FirebirdPacketCodecEngine;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.FirebirdCommandPacketType;
import org.apache.shardingsphere.database.protocol.firebird.packet.generic.FirebirdGenericResponsePacket;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.rule.RuleMetaData;
import org.apache.shardingsphere.infra.metadata.user.Grantee;
import org.apache.shardingsphere.infra.session.connection.ConnectionContext;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.handler.ProxySQLComQueryParser;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.backend.session.transaction.TransactionStatus;
import org.apache.shardingsphere.proxy.frontend.command.CommandExecutorTask;
import org.apache.shardingsphere.proxy.frontend.firebird.authentication.FirebirdAuthenticationEngine;
import org.apache.shardingsphere.proxy.frontend.firebird.command.FirebirdCommandExecuteEngine;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.transaction.FirebirdTransactionIdGenerator;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.transaction.FirebirdTransactionSessionRegistry;
import org.apache.shardingsphere.proxy.frontend.firebird.resource.FirebirdConnectionResourceManager;
import org.apache.shardingsphere.proxy.frontend.spi.DatabaseProtocolFrontendEngine;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.tcl.SetTransactionStatement;
import org.apache.shardingsphere.test.infra.framework.extension.mock.AutoMockExtension;
import org.apache.shardingsphere.test.infra.framework.extension.mock.StaticMockSettings;
import org.apache.shardingsphere.transaction.api.TransactionType;
import org.apache.shardingsphere.transaction.rule.TransactionRule;
import org.firebirdsql.gds.ISCConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(AutoMockExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@StaticMockSettings({ProxyContext.class, FirebirdConnectionResourceManager.class, ProxySQLComQueryParser.class})
class FirebirdFrontendEngineTest {
    
    private final DatabaseType databaseType = TypedSPILoader.getService(DatabaseType.class, "Firebird");
    
    private final DatabaseProtocolFrontendEngine engine = DatabaseTypedSPILoader.getService(DatabaseProtocolFrontendEngine.class, databaseType);
    
    @Mock
    private ConnectionSession connectionSession;
    
    @Mock
    private FirebirdConnectionResourceManager connectionResourceManager;
    
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ChannelHandlerContext channelHandlerContext;
    
    @Mock
    private Connection backendConnection;
    
    @Test
    void assertGetAuthenticationEngine() {
        assertThat(engine.getAuthenticationEngine(), isA(FirebirdAuthenticationEngine.class));
    }
    
    @Test
    void assertGetCommandExecuteEngine() {
        assertThat(engine.getCommandExecuteEngine(), isA(FirebirdCommandExecuteEngine.class));
    }
    
    @Test
    void assertGetCodecEngine() {
        assertThat(engine.getCodecEngine(), isA(FirebirdPacketCodecEngine.class));
    }
    
    @Test
    void assertRelease() {
        int connectionId = 1;
        when(connectionSession.getConnectionId()).thenReturn(connectionId);
        when(FirebirdConnectionResourceManager.getInstance()).thenReturn(connectionResourceManager);
        engine.release(connectionSession);
        verify(connectionResourceManager).unregisterConnection(connectionId);
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("provideHandleExceptionParameters")
    void assertHandleException(final String testName, final boolean inTransaction, final boolean exceptionOccur, final Exception exception, final boolean expectedExceptionOccur) {
        ConnectionContext connectionContext = mockConnectionSession(inTransaction, exceptionOccur);
        engine.handleException(connectionSession, exception);
        assertThat(connectionContext.getTransactionContext().isExceptionOccur(), is(expectedExceptionOccur));
    }
    
    @Test
    void assertActiveTransactionSurvivesRepeatedSetTransaction() throws SQLException {
        int connectionId = 3;
        FirebirdTransactionIdGenerator.getInstance().registerConnection(connectionId);
        try {
            ConnectionSession transactionSession = createConnectionSessionInTransaction(connectionId);
            int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(connectionId);
            FirebirdTransactionSessionRegistry.getInstance().register(transactionSession, transactionId);
            String sql = "SET TRANSACTION";
            byte[] sqlBytes = sql.getBytes(StandardCharsets.US_ASCII);
            when(ProxySQLComQueryParser.parse(sql, databaseType, transactionSession)).thenReturn(new SetTransactionStatement(databaseType));
            DatabaseProtocolFrontendEngine engine = DatabaseTypedSPILoader.getService(DatabaseProtocolFrontendEngine.class, databaseType);
            ByteBuf request = Unpooled.buffer().writeInt(FirebirdCommandPacketType.EXEC_IMMEDIATE.getValue()).writeInt(transactionId).writeInt(0).writeInt(3)
                    .writeInt(sqlBytes.length).writeBytes(sqlBytes).writeZero((4 - sqlBytes.length % 4) % 4).writeInt(0).writeInt(0);
            new CommandExecutorTask(engine, transactionSession, channelHandlerContext, request).run();
            new CommandExecutorTask(engine, transactionSession, channelHandlerContext, Unpooled.buffer().writeInt(FirebirdCommandPacketType.COMMIT.getValue()).writeInt(transactionId)).run();
            ArgumentCaptor<FirebirdGenericResponsePacket> responses = ArgumentCaptor.forClass(FirebirdGenericResponsePacket.class);
            verify(channelHandlerContext, times(2)).write(responses.capture());
            FirebirdGenericResponsePacket actualError = responses.getAllValues().get(0);
            assertThat(actualError.getHandle(), is(transactionId));
            assertThat(actualError.getStatusVector().getSegments().size(), is(1));
            assertThat(actualError.getStatusVector().getSegments().get(0).getGdsCode(), is(ISCConstants.isc_bad_trans_handle));
            assertTrue(actualError.getStatusVector().getSegments().get(0).getArguments().isEmpty());
            assertNull(responses.getAllValues().get(1).getStatusVector());
            verify(backendConnection).commit();
            verify(backendConnection, never()).rollback();
        } finally {
            FirebirdTransactionIdGenerator.getInstance().unregisterConnection(connectionId);
        }
    }
    
    @Test
    void assertActiveTransactionSurvivesDialectParsingError() throws SQLException {
        int connectionId = 3;
        FirebirdTransactionIdGenerator.getInstance().registerConnection(connectionId);
        try {
            ConnectionSession transactionSession = createConnectionSessionInTransaction(connectionId);
            int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(connectionId);
            FirebirdTransactionSessionRegistry.getInstance().register(transactionSession, transactionId);
            String sql = "SET TRANSACTION INVALID";
            byte[] sqlBytes = sql.getBytes(StandardCharsets.US_ASCII);
            when(ProxySQLComQueryParser.parse(sql, databaseType, transactionSession)).thenThrow(new DialectSQLParsingException("invalid", null, 1));
            DatabaseProtocolFrontendEngine engine = DatabaseTypedSPILoader.getService(DatabaseProtocolFrontendEngine.class, databaseType);
            ByteBuf request = Unpooled.buffer().writeInt(FirebirdCommandPacketType.EXEC_IMMEDIATE.getValue()).writeInt(transactionId).writeInt(0).writeInt(3)
                    .writeInt(sqlBytes.length).writeBytes(sqlBytes).writeZero((4 - sqlBytes.length % 4) % 4).writeInt(0).writeInt(0);
            new CommandExecutorTask(engine, transactionSession, channelHandlerContext, request).run();
            new CommandExecutorTask(engine, transactionSession, channelHandlerContext, Unpooled.buffer().writeInt(FirebirdCommandPacketType.COMMIT.getValue()).writeInt(transactionId)).run();
            ArgumentCaptor<FirebirdGenericResponsePacket> responses = ArgumentCaptor.forClass(FirebirdGenericResponsePacket.class);
            verify(channelHandlerContext, times(2)).write(responses.capture());
            FirebirdGenericResponsePacket actualError = responses.getAllValues().get(0);
            assertThat(actualError.getHandle(), is(transactionId));
            assertTrue(0 != actualError.getStatusVector().getSegments().get(0).getGdsCode());
            assertNull(responses.getAllValues().get(1).getStatusVector());
            verify(backendConnection).commit();
            verify(backendConnection, never()).rollback();
        } finally {
            FirebirdTransactionIdGenerator.getInstance().unregisterConnection(connectionId);
        }
    }
    
    private ConnectionSession createConnectionSessionInTransaction(final int connectionId) throws SQLException {
        TransactionRule transactionRule = mock(TransactionRule.class);
        when(transactionRule.getDefaultType()).thenReturn(TransactionType.LOCAL);
        when(ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData()).thenReturn(
                new ShardingSphereMetaData(Collections.emptyList(), mock(), new RuleMetaData(Collections.singleton(transactionRule)), new ConfigurationProperties(new Properties())));
        when(channelHandlerContext.channel().attr(CommonConstants.CHARSET_ATTRIBUTE_KEY).get()).thenReturn(StandardCharsets.UTF_8);
        ConnectionSession result = new ConnectionSession(databaseType, new DefaultAttributeMap());
        result.setConnectionId(connectionId);
        result.setGrantee(new Grantee("foo_user", "%"));
        result.setAutoCommit(false);
        result.getDatabaseConnectionManager().handleAutoCommit();
        result.getDatabaseConnectionManager().getCachedConnections().put("foo_db.foo_ds", backendConnection);
        return result;
    }
    
    private ConnectionContext mockConnectionSession(final boolean inTransaction, final boolean exceptionOccur) {
        TransactionStatus transactionStatus = new TransactionStatus();
        transactionStatus.setInTransaction(inTransaction);
        ConnectionContext result = new ConnectionContext(Collections::emptyList);
        result.getTransactionContext().setExceptionOccur(exceptionOccur);
        when(connectionSession.getTransactionStatus()).thenReturn(transactionStatus);
        if (inTransaction) {
            when(connectionSession.getConnectionContext()).thenReturn(result);
        }
        return result;
    }
    
    private static Stream<Arguments> provideHandleExceptionParameters() {
        return Stream.of(
                Arguments.of("mark when in transaction", true, false, new Exception("error"), true),
                Arguments.of("skip when in transaction exception", true, false, new InTransactionException(), false),
                Arguments.of("skip when not in transaction", false, false, new Exception("error"), false),
                Arguments.of("keep marked when already occurred", true, true, new Exception("error"), true));
    }
}
