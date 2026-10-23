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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query.batch;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.DefaultAttributeMap;
import org.apache.shardingsphere.database.connector.core.spi.DatabaseTypedSPILoader;
import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.database.exception.core.exception.protocol.DatabaseProtocolException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchAlreadyOpenedException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchParametersRequiredException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchMessageFormatException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchParameterVersionException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchStatementTypeException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidStatementHandleException;
import org.apache.shardingsphere.database.protocol.constant.CommonConstants;
import org.apache.shardingsphere.database.protocol.firebird.err.FirebirdErrorPacketFactory;
import org.apache.shardingsphere.database.protocol.firebird.err.FirebirdStatusVector.Segment;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.FirebirdCommandPacketType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.FirebirdBinaryColumnType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchCreateCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchRegistry;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchStatement;
import org.apache.shardingsphere.database.protocol.firebird.packet.generic.FirebirdGenericResponsePacket;
import org.apache.shardingsphere.database.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.rule.RuleMetaData;
import org.apache.shardingsphere.infra.metadata.user.Grantee;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.command.CommandExecutorTask;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.FirebirdServerPreparedStatement;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.transaction.FirebirdTransactionIdGenerator;
import org.apache.shardingsphere.proxy.frontend.spi.DatabaseProtocolFrontendEngine;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.ReturningSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.subquery.SubquerySegment;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.ddl.ExecuteStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.DeleteStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.InsertStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.MergeStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.SelectStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.UpdateStatement;
import org.apache.shardingsphere.test.infra.framework.extension.mock.AutoMockExtension;
import org.apache.shardingsphere.test.infra.framework.extension.mock.StaticMockSettings;
import org.apache.shardingsphere.transaction.api.TransactionType;
import org.apache.shardingsphere.transaction.rule.TransactionRule;
import org.firebirdsql.gds.BlrConstants;
import org.firebirdsql.gds.ISCConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(AutoMockExtension.class)
@StaticMockSettings(ProxyContext.class)
class FirebirdBatchCreateCommandExecutorTest {
    
    private static final int CONNECTION_ID = 1;
    
    private static final int STATEMENT_ID = 11;
    
    private static final long DEFAULT_BUFFER_SIZE = 16L * 1024 * 1024;
    
    private static final long MAX_BUFFER_SIZE = 256L * 1024 * 1024;
    
    private static final int BATCH_VERSION_1 = 1;
    
    private static final int TAG_MULTIERROR = 1;
    
    private static final int TAG_RECORD_COUNTS = 2;
    
    private static final int TAG_BUFFER_BYTES_SIZE = 3;
    
    private static final int TAG_BLOB_POLICY = 4;
    
    private static final int BLOB_ID_USER = 2;
    
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConnectionSession connectionSession;
    
    @Mock
    private FirebirdBatchCreateCommandPacket packet;
    
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private FirebirdServerPreparedStatement preparedStatement;
    
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ChannelHandlerContext channelHandlerContext;
    
    @Mock
    private Connection backendConnection;
    
    @AfterEach
    void tearDown() {
        FirebirdBatchRegistry.getInstance().unregisterConnection(CONNECTION_ID);
        FirebirdTransactionIdGenerator.getInstance().unregisterConnection(CONNECTION_ID);
    }
    
    @Test
    void assertExecute() throws SQLException {
        FirebirdBatchRegistry.getInstance().registerConnection(CONNECTION_ID);
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        when(packet.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(STATEMENT_ID)).thenReturn(preparedStatement);
        when(preparedStatement.getSqlStatementContext().getSqlStatement()).thenReturn(mock(InsertStatement.class));
        when(packet.getBatchBlr()).thenReturn(createBatchBlr());
        when(packet.getBatchMessageLength()).thenReturn(6L);
        when(packet.getBatchParametersBuffer()).thenReturn(Unpooled.EMPTY_BUFFER);
        Collection<DatabasePacket> actual = new FirebirdBatchCreateCommandExecutor(packet, connectionSession).execute();
        assertThat(actual.size(), is(1));
        DatabasePacket actualPacket = actual.iterator().next();
        assertThat(actualPacket, isA(FirebirdGenericResponsePacket.class));
        assertThat(((FirebirdGenericResponsePacket) actualPacket).getHandle(), is(STATEMENT_ID));
        FirebirdBatchStatement actualBatchStatement = FirebirdBatchRegistry.getInstance().getBatchStatement(CONNECTION_ID, STATEMENT_ID);
        assertNotNull(actualBatchStatement);
        assertThat(actualBatchStatement.getStatementHandle(), is(STATEMENT_ID));
        assertFalse(actualBatchStatement.isRecordCounts());
        assertFalse(actualBatchStatement.isMultiError());
        assertThat(actualBatchStatement.getColumnDescriptors().size(), is(1));
        assertThat(actualBatchStatement.getColumnDescriptors().get(0).getType(), is(FirebirdBinaryColumnType.LEGACY_TEXT));
        assertThat(actualBatchStatement.getColumnDescriptors().get(0).getLength(), is(4));
        assertThat(actualBatchStatement.getColumnDescriptors().get(0).getScale(), is(0));
        assertThat(actualBatchStatement.getColumnDescriptors().get(0).getOffset(), is(0));
    }
    
    @Test
    void assertExecuteWithRecordCounts() throws SQLException {
        FirebirdBatchRegistry.getInstance().registerConnection(CONNECTION_ID);
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        when(packet.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(STATEMENT_ID)).thenReturn(preparedStatement);
        when(preparedStatement.getSqlStatementContext().getSqlStatement()).thenReturn(mock(InsertStatement.class));
        when(packet.getBatchBlr()).thenReturn(createBatchBlr());
        when(packet.getBatchMessageLength()).thenReturn(6L);
        when(packet.getBatchParametersBuffer()).thenReturn(createBatchParametersBuffer(TAG_RECORD_COUNTS, 1));
        new FirebirdBatchCreateCommandExecutor(packet, connectionSession).execute();
        assertTrue(FirebirdBatchRegistry.getInstance().getBatchStatement(CONNECTION_ID, STATEMENT_ID).isRecordCounts());
    }
    
    @Test
    void assertExecuteWithMultiError() throws SQLException {
        FirebirdBatchRegistry.getInstance().registerConnection(CONNECTION_ID);
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        when(packet.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(STATEMENT_ID)).thenReturn(preparedStatement);
        when(preparedStatement.getSqlStatementContext().getSqlStatement()).thenReturn(mock(InsertStatement.class));
        when(packet.getBatchBlr()).thenReturn(createBatchBlr());
        when(packet.getBatchMessageLength()).thenReturn(6L);
        when(packet.getBatchParametersBuffer()).thenReturn(createBatchParametersBuffer(TAG_MULTIERROR, 1));
        new FirebirdBatchCreateCommandExecutor(packet, connectionSession).execute();
        assertTrue(FirebirdBatchRegistry.getInstance().getBatchStatement(CONNECTION_ID, STATEMENT_ID).isMultiError());
    }
    
    @Test
    void assertExecuteWhenBatchAlreadyOpen() {
        FirebirdBatchRegistry.getInstance().registerConnection(CONNECTION_ID);
        FirebirdBatchStatement expectedBatchStatement = new FirebirdBatchStatement(STATEMENT_ID);
        FirebirdBatchRegistry.getInstance().registerBatchStatement(CONNECTION_ID, STATEMENT_ID, expectedBatchStatement);
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        when(packet.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(STATEMENT_ID)).thenReturn(preparedStatement);
        assertThrows(BatchAlreadyOpenedException.class, () -> new FirebirdBatchCreateCommandExecutor(packet, connectionSession).execute());
        assertThat(FirebirdBatchRegistry.getInstance().getBatchStatement(CONNECTION_ID, STATEMENT_ID), is(expectedBatchStatement));
    }
    
    @Test
    void assertExecuteWhenStatementDoesNotExist() {
        FirebirdBatchRegistry.getInstance().registerConnection(CONNECTION_ID);
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        when(packet.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(STATEMENT_ID)).thenReturn(null);
        assertThrows(InvalidStatementHandleException.class, () -> new FirebirdBatchCreateCommandExecutor(packet, connectionSession).execute());
        verify(packet, never()).getBatchBlr();
    }
    
    @Test
    void assertExecuteWhenBatchBlrHasNoFields() {
        FirebirdBatchRegistry.getInstance().registerConnection(CONNECTION_ID);
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        when(packet.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(STATEMENT_ID)).thenReturn(preparedStatement);
        when(preparedStatement.getSqlStatementContext().getSqlStatement()).thenReturn(mock(InsertStatement.class));
        when(packet.getBatchBlr()).thenReturn(createEmptyBatchBlr());
        assertThrows(BatchParametersRequiredException.class, () -> new FirebirdBatchCreateCommandExecutor(packet, connectionSession).execute());
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("batchStatementCases")
    void assertExecuteWithBatchStatement(final String name, final SQLStatement sqlStatement) throws SQLException {
        mockBatchCreate(sqlStatement, createBatchBlr(), 6L);
        new FirebirdBatchCreateCommandExecutor(packet, connectionSession).execute();
        assertNotNull(FirebirdBatchRegistry.getInstance().getBatchStatement(CONNECTION_ID, STATEMENT_ID));
    }
    
    private void mockBatchCreate(final SQLStatement sqlStatement, final ByteBuf batchBlr, final long messageLength) {
        FirebirdBatchRegistry.getInstance().registerConnection(CONNECTION_ID);
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        when(packet.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(STATEMENT_ID)).thenReturn(preparedStatement);
        when(preparedStatement.getSqlStatementContext().getSqlStatement()).thenReturn(sqlStatement);
        when(packet.getBatchBlr()).thenReturn(batchBlr);
        when(packet.getBatchMessageLength()).thenReturn(messageLength);
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("nonBatchStatementCases")
    void assertExecuteWithNonBatchStatement(final String name, final SQLStatement sqlStatement) {
        mockBatchCreate(sqlStatement, createBatchBlr(), 6L);
        assertThrows(InvalidBatchStatementTypeException.class, () -> new FirebirdBatchCreateCommandExecutor(packet, connectionSession).execute());
        assertNull(FirebirdBatchRegistry.getInstance().getBatchStatement(CONNECTION_ID, STATEMENT_ID));
    }
    
    @Test
    void assertExecuteWithNonBatchStatementAndEmptyBatchBlr() {
        mockBatchCreate(mock(SelectStatement.class), createEmptyBatchBlr(), 0L);
        assertThrows(InvalidBatchStatementTypeException.class, () -> new FirebirdBatchCreateCommandExecutor(packet, connectionSession).execute());
    }
    
    @Test
    void assertExecuteWithNonBatchStatementAndBlobPolicy() {
        mockBatchCreate(mock(SelectStatement.class), createBatchBlr(), 6L);
        when(packet.getBatchParametersBuffer()).thenReturn(createBatchParametersBuffer(TAG_BLOB_POLICY, BLOB_ID_USER));
        assertThrows(InvalidBatchStatementTypeException.class, () -> new FirebirdBatchCreateCommandExecutor(packet, connectionSession).execute());
        assertNull(FirebirdBatchRegistry.getInstance().getBatchStatement(CONNECTION_ID, STATEMENT_ID));
    }
    
    @Test
    void assertExecuteWithNonBatchStatementAndInvalidBatchParametersVersion() {
        FirebirdBatchRegistry.getInstance().registerConnection(CONNECTION_ID);
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        when(packet.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(STATEMENT_ID)).thenReturn(preparedStatement);
        when(packet.getBatchBlr()).thenReturn(createBatchBlr());
        when(packet.getBatchMessageLength()).thenReturn(6L);
        when(packet.getBatchParametersBuffer()).thenReturn(Unpooled.wrappedBuffer(new byte[]{2}));
        assertThrows(InvalidBatchParameterVersionException.class, () -> new FirebirdBatchCreateCommandExecutor(packet, connectionSession).execute());
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("nonBatchStatementCases")
    void assertExecuteWithNonBatchStatementInTransaction(final String name, final SQLStatement sqlStatement) throws SQLException {
        ConnectionSession transactionSession = createConnectionSessionInTransaction();
        when(preparedStatement.getSqlStatementContext().getSqlStatement()).thenReturn(sqlStatement);
        transactionSession.getServerPreparedStatementRegistry().addPreparedStatement(STATEMENT_ID, preparedStatement);
        FirebirdBatchRegistry.getInstance().registerConnection(CONNECTION_ID);
        int transactionHandle = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        DatabaseProtocolFrontendEngine engine = DatabaseTypedSPILoader.getService(DatabaseProtocolFrontendEngine.class, TypedSPILoader.getService(DatabaseType.class, "Firebird"));
        new CommandExecutorTask(engine, transactionSession, channelHandlerContext, createBatchCreatePacket()).run();
        new CommandExecutorTask(engine, transactionSession, channelHandlerContext, Unpooled.buffer().writeInt(FirebirdCommandPacketType.BATCH_MSG.getValue()).writeInt(STATEMENT_ID).writeInt(0)).run();
        new CommandExecutorTask(engine, transactionSession, channelHandlerContext,
                Unpooled.buffer().writeInt(FirebirdCommandPacketType.BATCH_EXEC.getValue()).writeInt(STATEMENT_ID).writeInt(transactionHandle)).run();
        new CommandExecutorTask(engine, transactionSession, channelHandlerContext, Unpooled.buffer().writeInt(FirebirdCommandPacketType.BATCH_RLS.getValue()).writeInt(STATEMENT_ID)).run();
        new CommandExecutorTask(engine, transactionSession, channelHandlerContext, Unpooled.buffer().writeInt(FirebirdCommandPacketType.COMMIT.getValue()).writeInt(transactionHandle)).run();
        ArgumentCaptor<FirebirdGenericResponsePacket> responseCaptor = ArgumentCaptor.forClass(FirebirdGenericResponsePacket.class);
        verify(channelHandlerContext, times(5)).write(responseCaptor.capture());
        List<Segment> actualSegments = responseCaptor.getAllValues().get(0).getStatusVector().getSegments();
        assertThat(actualSegments.size(), is(3));
        assertThat(actualSegments.get(0).getGdsCode(), is(ISCConstants.isc_dsql_error));
        assertThat(actualSegments.get(1).getGdsCode(), is(ISCConstants.isc_sqlerr));
        assertThat(actualSegments.get(1).getArguments(), is(Collections.singletonList(-901)));
        assertThat(actualSegments.get(2).getGdsCode(), is(ISCConstants.isc_batch_type));
        assertThat(responseCaptor.getAllValues().get(1).getErrorCode(), is(ISCConstants.isc_bad_batch_handle));
        assertThat(responseCaptor.getAllValues().get(2).getErrorCode(), is(ISCConstants.isc_bad_batch_handle));
        assertThat(responseCaptor.getAllValues().get(3).getErrorCode(), is(ISCConstants.isc_bad_batch_handle));
        assertNull(responseCaptor.getAllValues().get(4).getStatusVector());
        assertNull(FirebirdBatchRegistry.getInstance().getBatchStatement(CONNECTION_ID, STATEMENT_ID));
        verify(backendConnection).commit();
        verify(backendConnection, never()).rollback();
    }
    
    private ConnectionSession createConnectionSessionInTransaction() {
        TransactionRule transactionRule = mock(TransactionRule.class);
        when(transactionRule.getDefaultType()).thenReturn(TransactionType.LOCAL);
        when(ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData()).thenReturn(
                new ShardingSphereMetaData(Collections.emptyList(), mock(), new RuleMetaData(Collections.singleton(transactionRule)), new ConfigurationProperties(new Properties())));
        when(channelHandlerContext.channel().attr(CommonConstants.CHARSET_ATTRIBUTE_KEY).get()).thenReturn(StandardCharsets.UTF_8);
        ConnectionSession result = new ConnectionSession(TypedSPILoader.getService(DatabaseType.class, "Firebird"), new DefaultAttributeMap());
        result.setConnectionId(CONNECTION_ID);
        result.setGrantee(new Grantee("foo_user", "%"));
        result.setAutoCommit(false);
        result.getDatabaseConnectionManager().handleAutoCommit();
        result.getDatabaseConnectionManager().getCachedConnections().put("foo_db.foo_ds", backendConnection);
        FirebirdTransactionIdGenerator.getInstance().registerConnection(CONNECTION_ID);
        return result;
    }
    
    private ByteBuf createBatchCreatePacket() {
        ByteBuf blr = createBatchBlr();
        int blrLength = blr.readableBytes();
        return Unpooled.buffer().writeInt(FirebirdCommandPacketType.BATCH_CREATE.getValue()).writeInt(STATEMENT_ID).writeInt(blrLength).writeBytes(blr).writeZero((4 - blrLength % 4) % 4)
                .writeInt(6).writeInt(0);
    }
    
    @Test
    void assertExecuteWhenMessageLengthMismatched() {
        FirebirdBatchRegistry.getInstance().registerConnection(CONNECTION_ID);
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        when(packet.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(STATEMENT_ID)).thenReturn(preparedStatement);
        when(packet.getBatchBlr()).thenReturn(createBatchBlr());
        when(packet.getBatchMessageLength()).thenReturn(5L);
        assertThrows(InvalidBatchMessageFormatException.class, () -> new FirebirdBatchCreateCommandExecutor(packet, connectionSession).execute());
    }
    
    @Test
    void assertExecuteWhenBatchBlrContainsBlob() {
        FirebirdBatchRegistry.getInstance().registerConnection(CONNECTION_ID);
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        when(packet.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(STATEMENT_ID)).thenReturn(preparedStatement);
        when(packet.getBatchBlr()).thenReturn(createBlobBatchBlr());
        DatabaseProtocolException actual = assertThrows(DatabaseProtocolException.class, () -> new FirebirdBatchCreateCommandExecutor(packet, connectionSession).execute());
        assertThat(actual.getMessage(), is("BLOB fields are not supported in Firebird batch operations"));
        assertNull(FirebirdBatchRegistry.getInstance().getBatchStatement(CONNECTION_ID, STATEMENT_ID));
    }
    
    @Test
    void assertBlobBatchCreateProducesFirebirdErrorResponseWithoutClosingChannel() {
        FirebirdBatchRegistry.getInstance().registerConnection(CONNECTION_ID);
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        when(packet.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(STATEMENT_ID)).thenReturn(preparedStatement);
        when(packet.getBatchBlr()).thenReturn(createBlobBatchBlr());
        DatabaseProtocolException cause = assertThrows(DatabaseProtocolException.class, () -> new FirebirdBatchCreateCommandExecutor(packet, connectionSession).execute());
        FirebirdGenericResponsePacket errorPacket = (FirebirdGenericResponsePacket) FirebirdErrorPacketFactory.newInstance(cause);
        assertThat(errorPacket.getErrorMessage(), containsString("BLOB fields are not supported in Firebird batch operations"));
        assertNull(FirebirdBatchRegistry.getInstance().getBatchStatement(CONNECTION_ID, STATEMENT_ID));
    }
    
    @Test
    void assertParseBatchParametersWithEmptyBuffer() {
        FirebirdBatchCreateCommandExecutor.BatchParameters actual = FirebirdBatchCreateCommandExecutor.BatchParameters.parse(Unpooled.EMPTY_BUFFER);
        assertThat(actual.getVersion(), is(BATCH_VERSION_1));
        assertThat(actual.getBufferSize(), is(DEFAULT_BUFFER_SIZE));
        assertFalse(actual.isRecordCounts());
        assertFalse(actual.isMultiError());
    }
    
    @Test
    void assertParseBatchParametersWithBufferSize() {
        FirebirdBatchCreateCommandExecutor.BatchParameters actual = FirebirdBatchCreateCommandExecutor.BatchParameters.parse(createBatchParametersBuffer(TAG_BUFFER_BYTES_SIZE, 1024));
        assertThat(actual.getBufferSize(), is(1024L));
    }
    
    @Test
    void assertParseBatchParametersWithMaximumBufferSize() {
        FirebirdBatchCreateCommandExecutor.BatchParameters actual = FirebirdBatchCreateCommandExecutor.BatchParameters.parse(createBatchParametersBuffer(TAG_BUFFER_BYTES_SIZE, 0));
        assertThat(actual.getBufferSize(), is(MAX_BUFFER_SIZE));
    }
    
    @Test
    void assertParseBatchParametersWithTooLargeBufferSize() {
        FirebirdBatchCreateCommandExecutor.BatchParameters actual = FirebirdBatchCreateCommandExecutor.BatchParameters.parse(createBatchParametersBuffer(TAG_BUFFER_BYTES_SIZE, -1));
        assertThat(actual.getBufferSize(), is(MAX_BUFFER_SIZE));
    }
    
    @Test
    void assertParseBatchParametersWithRecordCounts() {
        FirebirdBatchCreateCommandExecutor.BatchParameters actual = FirebirdBatchCreateCommandExecutor.BatchParameters.parse(createBatchParametersBuffer(TAG_RECORD_COUNTS, 1));
        assertTrue(actual.isRecordCounts());
    }
    
    @Test
    void assertParseBatchParametersWithMultiError() {
        FirebirdBatchCreateCommandExecutor.BatchParameters actual = FirebirdBatchCreateCommandExecutor.BatchParameters.parse(createBatchParametersBuffer(TAG_MULTIERROR, 1));
        assertTrue(actual.isMultiError());
    }
    
    @Test
    void assertParseBatchParametersWithDisabledMultiError() {
        FirebirdBatchCreateCommandExecutor.BatchParameters actual = FirebirdBatchCreateCommandExecutor.BatchParameters.parse(createBatchParametersBuffer(TAG_MULTIERROR, 0));
        assertFalse(actual.isMultiError());
    }
    
    @Test
    void assertParseBatchParametersWithBlobPolicy() {
        FirebirdBatchCreateCommandExecutor.BatchParameters actual = FirebirdBatchCreateCommandExecutor.BatchParameters.parse(createBatchParametersBuffer(TAG_BLOB_POLICY, BLOB_ID_USER));
        assertThat(actual.getVersion(), is(BATCH_VERSION_1));
        assertThat(actual.getBufferSize(), is(DEFAULT_BUFFER_SIZE));
        assertFalse(actual.isRecordCounts());
        assertFalse(actual.isMultiError());
    }
    
    @Test
    void assertParseBatchParametersWhenVersionInvalid() {
        assertThrows(InvalidBatchParameterVersionException.class, () -> FirebirdBatchCreateCommandExecutor.BatchParameters.parse(Unpooled.wrappedBuffer(new byte[]{2})));
    }
    
    @Test
    void assertParseBatchParametersWhenClumpletTruncated() {
        assertThrows(DatabaseProtocolException.class, () -> FirebirdBatchCreateCommandExecutor.BatchParameters.parse(Unpooled.wrappedBuffer(new byte[]{BATCH_VERSION_1, TAG_BUFFER_BYTES_SIZE})));
    }
    
    @Test
    void assertParseBatchParametersWhenValueTruncated() {
        assertThrows(DatabaseProtocolException.class,
                () -> FirebirdBatchCreateCommandExecutor.BatchParameters.parse(Unpooled.buffer().writeByte(BATCH_VERSION_1).writeByte(TAG_BUFFER_BYTES_SIZE).writeIntLE(4)));
    }
    
    @Test
    void assertParseBatchParametersWhenIntegerLengthInvalid() {
        assertThrows(DatabaseProtocolException.class,
                () -> FirebirdBatchCreateCommandExecutor.BatchParameters.parse(Unpooled.buffer().writeByte(BATCH_VERSION_1).writeByte(TAG_BUFFER_BYTES_SIZE).writeIntLE(1).writeByte(1)));
    }
    
    @Test
    void assertParseBatchParametersWithUnknownTag() {
        FirebirdBatchCreateCommandExecutor.BatchParameters actual =
                FirebirdBatchCreateCommandExecutor.BatchParameters.parse(Unpooled.buffer().writeByte(BATCH_VERSION_1).writeByte(99).writeIntLE(1).writeByte(1));
        assertThat(actual.getVersion(), is(BATCH_VERSION_1));
        assertThat(actual.getBufferSize(), is(DEFAULT_BUFFER_SIZE));
        assertFalse(actual.isRecordCounts());
    }
    
    private static Stream<Arguments> batchStatementCases() {
        InsertStatement insertWithReturning = mock(InsertStatement.class);
        when(insertWithReturning.getReturning()).thenReturn(Optional.of(mock(ReturningSegment.class)));
        return Stream.of(
                Arguments.of("insert", mock(InsertStatement.class)),
                Arguments.of("insert values with returning", insertWithReturning),
                Arguments.of("update", mock(UpdateStatement.class)),
                Arguments.of("delete", mock(DeleteStatement.class)),
                Arguments.of("merge", mock(MergeStatement.class)),
                Arguments.of("execute procedure", mock(ExecuteStatement.class)));
    }
    
    private static Stream<Arguments> nonBatchStatementCases() {
        InsertStatement insertSelectWithReturning = mock(InsertStatement.class);
        when(insertSelectWithReturning.getReturning()).thenReturn(Optional.of(mock(ReturningSegment.class)));
        when(insertSelectWithReturning.getInsertSelect()).thenReturn(Optional.of(mock(SubquerySegment.class)));
        UpdateStatement updateWithReturning = mock(UpdateStatement.class);
        when(updateWithReturning.getReturning()).thenReturn(Optional.of(mock(ReturningSegment.class)));
        DeleteStatement deleteWithReturning = mock(DeleteStatement.class);
        when(deleteWithReturning.getReturning()).thenReturn(Optional.of(mock(ReturningSegment.class)));
        MergeStatement mergeWithReturning = mock(MergeStatement.class);
        when(mergeWithReturning.getReturning()).thenReturn(Optional.of(mock(ReturningSegment.class)));
        ExecuteStatement executeBlockWithReturning = mock(ExecuteStatement.class);
        when(executeBlockWithReturning.isReturningRows()).thenReturn(true);
        return Stream.of(
                Arguments.of("select", mock(SelectStatement.class)),
                Arguments.of("insert select with returning", insertSelectWithReturning),
                Arguments.of("update with returning", updateWithReturning),
                Arguments.of("delete with returning", deleteWithReturning),
                Arguments.of("merge with returning", mergeWithReturning),
                Arguments.of("execute block with returning", executeBlockWithReturning),
                Arguments.of("other statement", mock(SQLStatement.class)));
    }
    
    private ByteBuf createBatchBlr() {
        return Unpooled.wrappedBuffer(new byte[]{
                (byte) BlrConstants.blr_version5, (byte) BlrConstants.blr_begin, (byte) BlrConstants.blr_message, 0,
                2, 0, (byte) BlrConstants.blr_text, 4, 0, (byte) BlrConstants.blr_short, 0, (byte) BlrConstants.blr_end, (byte) BlrConstants.blr_eoc});
    }
    
    private ByteBuf createEmptyBatchBlr() {
        return Unpooled.wrappedBuffer(new byte[]{
                (byte) BlrConstants.blr_version5, (byte) BlrConstants.blr_begin, (byte) BlrConstants.blr_message, 0, 0, 0, (byte) BlrConstants.blr_end, (byte) BlrConstants.blr_eoc});
    }
    
    private ByteBuf createBlobBatchBlr() {
        return Unpooled.wrappedBuffer(new byte[]{
                (byte) BlrConstants.blr_version5, (byte) BlrConstants.blr_begin, (byte) BlrConstants.blr_message, 0,
                2, 0, (byte) BlrConstants.blr_blob2, 0, 0, 0, 0, (byte) BlrConstants.blr_short, 0, (byte) BlrConstants.blr_end, (byte) BlrConstants.blr_eoc});
    }
    
    private ByteBuf createBatchParametersBuffer(final int tag, final int value) {
        return Unpooled.buffer().writeByte(BATCH_VERSION_1).writeByte(tag).writeIntLE(4).writeIntLE(value);
    }
}
