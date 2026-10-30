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

import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.database.exception.core.exception.syntax.sql.DialectSQLParsingException;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.execute.FirebirdExecuteImmediatePacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.generic.FirebirdGenericResponsePacket;
import org.apache.shardingsphere.database.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.infra.hint.HintValueContext;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.proxy.backend.handler.ProxyBackendHandler;
import org.apache.shardingsphere.proxy.backend.handler.ProxyBackendHandlerFactory;
import org.apache.shardingsphere.proxy.backend.handler.ProxySQLComQueryParser;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.transaction.FirebirdTransactionIdGenerator;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.transaction.FirebirdTransactionSessionRegistry;
import org.apache.shardingsphere.sql.parser.engine.exception.SQLParsingException;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dcl.role.SetRoleStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.ddl.table.CreateTableStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.DeleteStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.InsertStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.SelectStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.UpdateStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.tcl.CommitStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.tcl.RollbackStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.tcl.SetTransactionStatement;
import org.apache.shardingsphere.test.infra.framework.extension.mock.AutoMockExtension;
import org.apache.shardingsphere.test.infra.framework.extension.mock.StaticMockSettings;
import org.firebirdsql.gds.ISCConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Answers;
import org.mockito.Mock;

import java.sql.SQLException;
import java.util.Collection;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(AutoMockExtension.class)
@StaticMockSettings({ProxySQLComQueryParser.class, ProxyBackendHandlerFactory.class})
class FirebirdExecuteImmediateCommandExecutorTest {
    
    private static final int CONNECTION_ID = 3;
    
    private static final DatabaseType DATABASE_TYPE = TypedSPILoader.getService(DatabaseType.class, "Firebird");
    
    @Mock
    private FirebirdExecuteImmediatePacket packet;
    
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConnectionSession connectionSession;
    
    @Mock
    private ProxyBackendHandler proxyBackendHandler;
    
    @BeforeEach
    void setUp() {
        FirebirdTransactionIdGenerator.getInstance().registerConnection(CONNECTION_ID);
    }
    
    @AfterEach
    void tearDown() {
        FirebirdTransactionIdGenerator.getInstance().unregisterConnection(CONNECTION_ID);
    }
    
    @Test
    void assertExecuteStartsTransaction() throws SQLException {
        mockSetTransaction();
        Collection<DatabasePacket> actual = new FirebirdExecuteImmediateCommandExecutor(packet, connectionSession).execute();
        int actualHandle = ((FirebirdGenericResponsePacket) actual.iterator().next()).getHandle();
        assertThat(actualHandle, is(1));
        assertTrue(FirebirdTransactionIdGenerator.getInstance().isTransactionActive(CONNECTION_ID, actualHandle));
    }
    
    @Test
    void assertExecuteWithActiveTransaction() throws SQLException {
        mockSetTransaction();
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        when(packet.getTransactionId()).thenReturn(transactionId);
        Collection<DatabasePacket> actual = new FirebirdExecuteImmediateCommandExecutor(packet, connectionSession).execute();
        FirebirdGenericResponsePacket actualResponse = (FirebirdGenericResponsePacket) actual.iterator().next();
        assertThat(actualResponse.getHandle(), is(transactionId));
        assertThat(actualResponse.getStatusVector().getSegments().size(), is(1));
        assertThat(actualResponse.getStatusVector().getSegments().get(0).getGdsCode(), is(ISCConstants.isc_bad_trans_handle));
        assertTrue(actualResponse.getStatusVector().getSegments().get(0).getArguments().isEmpty());
        assertTrue(FirebirdTransactionIdGenerator.getInstance().isTransactionActive(CONNECTION_ID, transactionId));
    }
    
    private void mockSetTransaction() {
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        when(packet.getSQL()).thenReturn("SET TRANSACTION");
        when(ProxySQLComQueryParser.parse("SET TRANSACTION", DATABASE_TYPE, connectionSession)).thenReturn(new SetTransactionStatement(DATABASE_TYPE));
    }
    
    @Test
    void assertExecuteStatementInTransaction() throws SQLException {
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        SQLStatement statement = InsertStatement.builder().databaseType(DATABASE_TYPE).build();
        mockStatement("INSERT INTO T VALUES (1)", statement, transactionId);
        Collection<DatabasePacket> actual = new FirebirdExecuteImmediateCommandExecutor(packet, connectionSession).execute();
        assertThat(((FirebirdGenericResponsePacket) actual.iterator().next()).getHandle(), is(transactionId));
        verify(proxyBackendHandler).execute();
    }
    
    @Test
    void assertExecuteCommit() throws SQLException {
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        mockStatement("COMMIT", new CommitStatement(DATABASE_TYPE), transactionId);
        Collection<DatabasePacket> actual = new FirebirdExecuteImmediateCommandExecutor(packet, connectionSession).execute();
        assertThat(((FirebirdGenericResponsePacket) actual.iterator().next()).getHandle(), is(0));
        assertFalse(FirebirdTransactionIdGenerator.getInstance().isTransactionActive(CONNECTION_ID, transactionId));
    }
    
    @Test
    void assertExecuteRollback() throws SQLException {
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        mockStatement("ROLLBACK", new RollbackStatement(DATABASE_TYPE), transactionId);
        Collection<DatabasePacket> actual = new FirebirdExecuteImmediateCommandExecutor(packet, connectionSession).execute();
        assertThat(((FirebirdGenericResponsePacket) actual.iterator().next()).getHandle(), is(0));
        assertFalse(FirebirdTransactionIdGenerator.getInstance().isTransactionActive(CONNECTION_ID, transactionId));
    }
    
    private void mockStatement(final String sql, final SQLStatement statement, final int transactionId) throws SQLException {
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        FirebirdTransactionSessionRegistry.getInstance().register(connectionSession, transactionId);
        when(packet.getSQL()).thenReturn(sql);
        when(packet.getTransactionId()).thenReturn(transactionId);
        when(packet.getHintValueContext()).thenReturn(new HintValueContext());
        when(ProxySQLComQueryParser.parse(sql, DATABASE_TYPE, connectionSession)).thenReturn(statement);
        when(ProxyBackendHandlerFactory.newInstance(eq(DATABASE_TYPE), eq(sql), eq(statement), eq(connectionSession), any(HintValueContext.class))).thenReturn(proxyBackendHandler);
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("statementsWithoutTransaction")
    void assertExecuteStatementWithoutTransaction(final String sql, final SQLStatement statement) throws SQLException {
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        when(packet.getSQL()).thenReturn(sql);
        when(ProxySQLComQueryParser.parse(sql, DATABASE_TYPE, connectionSession)).thenReturn(statement);
        Collection<DatabasePacket> actual = new FirebirdExecuteImmediateCommandExecutor(packet, connectionSession).execute();
        FirebirdGenericResponsePacket actualResponse = (FirebirdGenericResponsePacket) actual.iterator().next();
        assertThat(actualResponse.getHandle(), is(0));
        assertThat(actualResponse.getStatusVector().getSegments().size(), is(3));
        assertThat(actualResponse.getStatusVector().getSegments().get(0).getGdsCode(), is(ISCConstants.isc_dsql_error));
        assertThat(actualResponse.getStatusVector().getSegments().get(1).getGdsCode(), is(ISCConstants.isc_sqlerr));
        assertThat(actualResponse.getStatusVector().getSegments().get(1).getTypedArguments().get(0).getNumber(), is(-901));
        assertThat(actualResponse.getStatusVector().getSegments().get(2).getGdsCode(), is(ISCConstants.isc_bad_trans_handle));
    }
    
    private static Stream<Arguments> statementsWithoutTransaction() {
        return Stream.of(Arguments.of("INSERT INTO T VALUES (1)", InsertStatement.builder().databaseType(DATABASE_TYPE).build()),
                Arguments.of("UPDATE T SET A = 1", UpdateStatement.builder().databaseType(DATABASE_TYPE).build()),
                Arguments.of("DELETE FROM T", DeleteStatement.builder().databaseType(DATABASE_TYPE).build()),
                Arguments.of("SELECT A FROM T", SelectStatement.builder().databaseType(DATABASE_TYPE).build()),
                Arguments.of("CREATE TABLE T (A INTEGER)", CreateTableStatement.builder().databaseType(DATABASE_TYPE).build()));
    }
    
    @Test
    void assertExecuteStatementWithInvalidNonzeroTransaction() throws SQLException {
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        when(packet.getTransactionId()).thenReturn(123);
        when(packet.getSQL()).thenReturn("INSERT INTO T VALUES (1)");
        when(ProxySQLComQueryParser.parse("INSERT INTO T VALUES (1)", DATABASE_TYPE, connectionSession)).thenReturn(InsertStatement.builder().databaseType(DATABASE_TYPE).build());
        FirebirdGenericResponsePacket actual = (FirebirdGenericResponsePacket) new FirebirdExecuteImmediateCommandExecutor(packet, connectionSession).execute().iterator().next();
        assertThat(actual.getStatusVector().getSegments().size(), is(1));
        assertThat(actual.getStatusVector().getSegments().get(0).getGdsCode(), is(ISCConstants.isc_bad_trans_handle));
    }
    
    @Test
    void assertSessionStatementWithoutTransactionKeepsExistingResponse() throws SQLException {
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        when(packet.getSQL()).thenReturn("SET ROLE FOO");
        when(ProxySQLComQueryParser.parse("SET ROLE FOO", DATABASE_TYPE, connectionSession)).thenReturn(new SetRoleStatement(DATABASE_TYPE));
        FirebirdGenericResponsePacket actual = (FirebirdGenericResponsePacket) new FirebirdExecuteImmediateCommandExecutor(packet, connectionSession).execute().iterator().next();
        assertThat(actual.getStatusVector().getSegments().size(), is(1));
        assertThat(actual.getStatusVector().getSegments().get(0).getGdsCode(), is(ISCConstants.isc_bad_trans_handle));
    }
    
    @Test
    void assertParsingErrorKeepsActiveTransaction() throws SQLException {
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        when(packet.getTransactionId()).thenReturn(transactionId);
        when(packet.getSQL()).thenReturn("SET TRANSACTION INVALID");
        when(ProxySQLComQueryParser.parse("SET TRANSACTION INVALID", DATABASE_TYPE, connectionSession)).thenThrow(new SQLParsingException("SET TRANSACTION INVALID"));
        FirebirdGenericResponsePacket actual = (FirebirdGenericResponsePacket) new FirebirdExecuteImmediateCommandExecutor(packet, connectionSession).execute().iterator().next();
        assertThat(actual.getHandle(), is(transactionId));
        assertTrue(0 != actual.getStatusVector().getSegments().get(0).getGdsCode());
        assertTrue(FirebirdTransactionIdGenerator.getInstance().isTransactionActive(CONNECTION_ID, transactionId));
    }
    
    @Test
    void assertDialectParsingErrorKeepsActiveTransaction() throws SQLException {
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        when(packet.getTransactionId()).thenReturn(transactionId);
        when(packet.getSQL()).thenReturn("SET TRANSACTION INVALID");
        when(ProxySQLComQueryParser.parse("SET TRANSACTION INVALID", DATABASE_TYPE, connectionSession)).thenThrow(new DialectSQLParsingException("invalid", null, 1));
        FirebirdGenericResponsePacket actual = (FirebirdGenericResponsePacket) new FirebirdExecuteImmediateCommandExecutor(packet, connectionSession).execute().iterator().next();
        assertThat(actual.getHandle(), is(transactionId));
        assertTrue(0 != actual.getStatusVector().getSegments().get(0).getGdsCode());
        assertTrue(FirebirdTransactionIdGenerator.getInstance().isTransactionActive(CONNECTION_ID, transactionId));
    }
    
    @ParameterizedTest
    @ValueSource(strings = {"SAVEPOINT SP1", "ROLLBACK TO \"SP1\"", "RELEASE SAVEPOINT SP1"})
    void assertSavepointRetainsUnsupportedResponse(final String sql) throws SQLException {
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        when(packet.getSQL()).thenReturn(sql);
        FirebirdGenericResponsePacket actual = (FirebirdGenericResponsePacket) new FirebirdExecuteImmediateCommandExecutor(packet, connectionSession).execute().iterator().next();
        assertThat(actual.getHandle(), is(0));
        assertTrue(FirebirdTransactionIdGenerator.getInstance().isTransactionActive(CONNECTION_ID, transactionId));
    }
}
