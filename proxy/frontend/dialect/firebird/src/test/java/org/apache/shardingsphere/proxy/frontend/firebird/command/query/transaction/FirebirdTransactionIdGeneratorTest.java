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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query.transaction;

import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.ExcessTransactionsException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidTransactionHandleException;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.backend.session.transaction.TransactionStatus;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.statement.FirebirdStatementIdGenerator;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.UpdateStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.tcl.CommitStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.tcl.RollbackStatement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FirebirdTransactionIdGeneratorTest {
    
    private static final DatabaseType DATABASE_TYPE = TypedSPILoader.getService(DatabaseType.class, "Firebird");
    
    private static final int CONNECTION_ID = 1;
    
    @BeforeEach
    void setup() {
        FirebirdTransactionIdGenerator.getInstance().registerConnection(CONNECTION_ID);
    }
    
    @AfterEach
    void tearDown() {
        FirebirdTransactionIdGenerator.getInstance().unregisterConnection(CONNECTION_ID);
    }
    
    @Test
    void assertNextTransactionId() {
        assertThat(FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID), is(1));
        assertThat(FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID), is(2));
    }
    
    @Test
    void assertNextTransactionIdAfterStatementId() {
        FirebirdStatementIdGenerator.getInstance().registerConnection(CONNECTION_ID);
        int statementId = FirebirdStatementIdGenerator.getInstance().nextStatementId(CONNECTION_ID);
        assertThat(FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID), is(statementId + 1));
        FirebirdStatementIdGenerator.getInstance().unregisterConnection(CONNECTION_ID);
    }
    
    @Test
    void assertGetTransactionId() {
        assertThat(FirebirdTransactionIdGenerator.getInstance().getTransactionId(CONNECTION_ID), is(0));
    }
    
    @Test
    void assertGetTransactionIdWithoutRegisteredConnection() {
        FirebirdTransactionIdGenerator.getInstance().unregisterConnection(CONNECTION_ID);
        assertThrows(IllegalStateException.class, () -> FirebirdTransactionIdGenerator.getInstance().getTransactionId(CONNECTION_ID));
    }
    
    @Test
    void assertIsTransactionActiveWithClosedTransaction() {
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        assertTrue(FirebirdTransactionIdGenerator.getInstance().isTransactionActive(CONNECTION_ID, transactionId));
        FirebirdTransactionIdGenerator.getInstance().closeTransaction(CONNECTION_ID, transactionId);
        assertFalse(FirebirdTransactionIdGenerator.getInstance().isTransactionActive(CONNECTION_ID, transactionId));
    }
    
    @Test
    void assertIsTransactionActiveWithoutRegisteredConnection() {
        FirebirdTransactionIdGenerator.getInstance().unregisterConnection(CONNECTION_ID);
        assertThrows(IllegalStateException.class, () -> FirebirdTransactionIdGenerator.getInstance().isTransactionActive(CONNECTION_ID, 1));
    }
    
    @Test
    void assertCheckTransactionForStatementWithoutBackendTransaction() {
        FirebirdTransactionIdGenerator.getInstance().updateBackendTransactionOwner(mockConnectionSession(true), FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID));
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        assertDoesNotThrow(() -> FirebirdTransactionIdGenerator.getInstance().checkTransactionForStatement(mockConnectionSession(false), transactionId));
    }
    
    @Test
    void assertCheckTransactionForStatementWithoutOwner() {
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        assertDoesNotThrow(() -> FirebirdTransactionIdGenerator.getInstance().checkTransactionForStatement(mockConnectionSession(true), transactionId));
    }
    
    @Test
    void assertCheckTransactionForStatementOfOwner() {
        ConnectionSession connectionSession = mockConnectionSession(true);
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        FirebirdTransactionIdGenerator.getInstance().updateBackendTransactionOwner(connectionSession, transactionId);
        assertDoesNotThrow(() -> FirebirdTransactionIdGenerator.getInstance().checkTransactionForStatement(connectionSession, transactionId));
    }
    
    @Test
    void assertCheckTransactionForStatementWithInactiveTransaction() {
        assertThrows(InvalidTransactionHandleException.class, () -> FirebirdTransactionIdGenerator.getInstance().checkTransactionForStatement(mockConnectionSession(false), 0));
    }
    
    @Test
    void assertCheckTransactionForStatementWithBackendTransactionOfAnotherTransaction() {
        ConnectionSession connectionSession = mockConnectionSession(true);
        FirebirdTransactionIdGenerator.getInstance().updateBackendTransactionOwner(connectionSession, FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID));
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        assertThrows(ExcessTransactionsException.class, () -> FirebirdTransactionIdGenerator.getInstance().checkTransactionForStatement(connectionSession, transactionId));
    }
    
    @Test
    void assertUpdateBackendTransactionOwnerAfterBackendTransactionEnded() {
        int ownerTransactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        FirebirdTransactionIdGenerator.getInstance().updateBackendTransactionOwner(mockConnectionSession(true), ownerTransactionId);
        FirebirdTransactionIdGenerator.getInstance().updateBackendTransactionOwner(mockConnectionSession(false), ownerTransactionId);
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        assertDoesNotThrow(() -> FirebirdTransactionIdGenerator.getInstance().checkTransactionForStatement(mockConnectionSession(true), transactionId));
    }
    
    @Test
    void assertCloseTransactionEndedByCommitStatement() {
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        assertThat(FirebirdTransactionIdGenerator.getInstance().closeTransactionEndedBy(CONNECTION_ID, transactionId, new CommitStatement(DATABASE_TYPE)), is(0));
        assertFalse(FirebirdTransactionIdGenerator.getInstance().isTransactionActive(CONNECTION_ID, transactionId));
    }
    
    @Test
    void assertCloseTransactionEndedByRollbackStatement() {
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        assertThat(FirebirdTransactionIdGenerator.getInstance().closeTransactionEndedBy(CONNECTION_ID, transactionId, new RollbackStatement(DATABASE_TYPE)), is(0));
        assertFalse(FirebirdTransactionIdGenerator.getInstance().isTransactionActive(CONNECTION_ID, transactionId));
    }
    
    @Test
    void assertCloseTransactionEndedByRollbackToSavepointStatement() {
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        assertThat(FirebirdTransactionIdGenerator.getInstance().closeTransactionEndedBy(CONNECTION_ID, transactionId, new RollbackStatement(DATABASE_TYPE, "foo_savepoint")), is(transactionId));
        assertTrue(FirebirdTransactionIdGenerator.getInstance().isTransactionActive(CONNECTION_ID, transactionId));
    }
    
    @Test
    void assertCloseTransactionEndedByUpdateStatement() {
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        UpdateStatement updateStatement = UpdateStatement.builder().databaseType(DATABASE_TYPE).build();
        assertThat(FirebirdTransactionIdGenerator.getInstance().closeTransactionEndedBy(CONNECTION_ID, transactionId, updateStatement), is(transactionId));
        assertTrue(FirebirdTransactionIdGenerator.getInstance().isTransactionActive(CONNECTION_ID, transactionId));
    }
    
    @Test
    void assertCloseTransactionReleasesBackendTransaction() {
        ConnectionSession connectionSession = mockConnectionSession(true);
        int ownerTransactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        int transactionId = FirebirdTransactionIdGenerator.getInstance().nextTransactionId(CONNECTION_ID);
        FirebirdTransactionIdGenerator.getInstance().updateBackendTransactionOwner(connectionSession, ownerTransactionId);
        FirebirdTransactionIdGenerator.getInstance().closeTransaction(CONNECTION_ID, ownerTransactionId);
        assertDoesNotThrow(() -> FirebirdTransactionIdGenerator.getInstance().checkTransactionForStatement(connectionSession, transactionId));
    }
    
    private ConnectionSession mockConnectionSession(final boolean inTransaction) {
        ConnectionSession result = mock(ConnectionSession.class);
        when(result.getConnectionId()).thenReturn(CONNECTION_ID);
        TransactionStatus transactionStatus = new TransactionStatus();
        transactionStatus.setInTransaction(inTransaction);
        when(result.getTransactionStatus()).thenReturn(transactionStatus);
        return result;
    }
}
