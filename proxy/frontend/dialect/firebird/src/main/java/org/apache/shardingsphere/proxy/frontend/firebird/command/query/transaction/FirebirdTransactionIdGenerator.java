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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.ExcessTransactionsException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidTransactionHandleException;
import org.apache.shardingsphere.infra.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.FirebirdObjectHandleGenerator;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.tcl.CommitStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.tcl.RollbackStatement;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Transaction ID generator for Firebird.
 */
@NoArgsConstructor(access = AccessLevel.NONE)
public final class FirebirdTransactionIdGenerator {
    
    private static final FirebirdTransactionIdGenerator INSTANCE = new FirebirdTransactionIdGenerator();
    
    private static final int MAX_BACKEND_TRANSACTIONS = 1;
    
    private final Map<Integer, AtomicInteger> connectionRegistry = new ConcurrentHashMap<>();
    
    private final Map<Integer, Set<Integer>> activeTransactionRegistry = new ConcurrentHashMap<>();
    
    private final Map<Integer, Integer> backendTransactionOwnerRegistry = new ConcurrentHashMap<>();
    
    /**
     * Get prepared transaction registry instance.
     *
     * @return prepared statement registry instance
     */
    public static FirebirdTransactionIdGenerator getInstance() {
        return INSTANCE;
    }
    
    /**
     * Register connection.
     *
     * @param connectionId connection ID
     */
    public void registerConnection(final int connectionId) {
        connectionRegistry.put(connectionId, new AtomicInteger());
        activeTransactionRegistry.put(connectionId, ConcurrentHashMap.newKeySet());
    }
    
    /**
     * Generate next transaction ID for connection.
     *
     * <p>Transaction IDs are issued from the object handles of the connection.</p>
     *
     * @param connectionId connection ID
     * @return generated transaction ID
     */
    public int nextTransactionId(final int connectionId) {
        AtomicInteger lastTransactionId = getTransactionCounter(connectionId);
        int result = FirebirdObjectHandleGenerator.getInstance().nextHandle(connectionId);
        getActiveTransactions(connectionId).add(result);
        lastTransactionId.set(result);
        return result;
    }
    
    /**
     * Judge whether transaction is active for connection.
     *
     * @param connectionId connection ID
     * @param transactionId transaction ID
     * @return is transaction active or not
     */
    public boolean isTransactionActive(final int connectionId, final int transactionId) {
        return getActiveTransactions(connectionId).contains(transactionId);
    }
    
    /**
     * Judge whether transaction can use the backend transaction of connection.
     *
     * <p>All transactions of a connection run in the single backend transaction of its session. The backend transaction belongs to the transaction
     * which executed the first statement in it, so another transaction of the connection can use it only after the owner commits or rolls back.</p>
     *
     * @param connectionSession connection session
     * @param transactionId transaction ID
     * @return can use the backend transaction or not
     */
    public boolean isBackendTransactionAvailable(final ConnectionSession connectionSession, final int transactionId) {
        Integer owner = backendTransactionOwnerRegistry.get(connectionSession.getConnectionId());
        return !connectionSession.getTransactionStatus().isInTransaction() || null == owner || owner == transactionId;
    }
    
    /**
     * Check that transaction can execute statements.
     *
     * @param connectionSession connection session
     * @param transactionId transaction ID
     * @throws InvalidTransactionHandleException if transaction is not active
     * @throws ExcessTransactionsException if the backend transaction belongs to another transaction
     */
    public void checkTransactionForStatement(final ConnectionSession connectionSession, final int transactionId) {
        ShardingSpherePreconditions.checkState(isTransactionActive(connectionSession.getConnectionId(), transactionId), () -> new InvalidTransactionHandleException(transactionId));
        ShardingSpherePreconditions.checkState(isBackendTransactionAvailable(connectionSession, transactionId), () -> new ExcessTransactionsException(MAX_BACKEND_TRANSACTIONS));
    }
    
    /**
     * Update owner of the backend transaction of connection after transaction executed a statement.
     *
     * @param connectionSession connection session
     * @param transactionId transaction ID
     */
    public void updateBackendTransactionOwner(final ConnectionSession connectionSession, final int transactionId) {
        if (connectionSession.getTransactionStatus().isInTransaction()) {
            backendTransactionOwnerRegistry.put(connectionSession.getConnectionId(), transactionId);
        } else {
            backendTransactionOwnerRegistry.remove(connectionSession.getConnectionId());
        }
    }
    
    /**
     * Close transaction if the statement executed in it ended it.
     *
     * <p>A COMMIT or ROLLBACK statement ends the transaction it was executed in, so the transaction ID becomes invalid as in Firebird.</p>
     *
     * @param connectionId connection ID
     * @param transactionId transaction ID
     * @param sqlStatement executed SQL statement
     * @return transaction ID after the statement, or {@code 0} if the statement ended the transaction
     */
    public int closeTransactionEndedBy(final int connectionId, final int transactionId, final SQLStatement sqlStatement) {
        if (sqlStatement instanceof CommitStatement || sqlStatement instanceof RollbackStatement && !((RollbackStatement) sqlStatement).getSavepointName().isPresent()) {
            closeTransaction(connectionId, transactionId);
            return 0;
        }
        return transactionId;
    }
    
    /**
     * Close transaction for connection.
     *
     * @param connectionId connection ID
     * @param transactionId transaction ID
     */
    public void closeTransaction(final int connectionId, final int transactionId) {
        if (getActiveTransactions(connectionId).remove(transactionId)) {
            FirebirdObjectHandleGenerator.getInstance().releaseHandle(connectionId, transactionId);
        }
        backendTransactionOwnerRegistry.remove(connectionId, transactionId);
    }
    
    /**
     * Get the last generated transaction ID for connection.
     *
     * @param connectionId connection ID
     * @return transaction ID
     */
    public int getTransactionId(final int connectionId) {
        return getTransactionCounter(connectionId).get();
    }
    
    /**
     * Unregister connection.
     *
     * @param connectionId connection ID
     */
    public void unregisterConnection(final int connectionId) {
        connectionRegistry.remove(connectionId);
        activeTransactionRegistry.remove(connectionId);
        backendTransactionOwnerRegistry.remove(connectionId);
        FirebirdObjectHandleGenerator.getInstance().unregisterConnection(connectionId);
    }
    
    private AtomicInteger getTransactionCounter(final int connectionId) {
        AtomicInteger result = connectionRegistry.get(connectionId);
        if (null == result) {
            throw new IllegalStateException("No transaction ID generator found for connectionId: " + connectionId);
        }
        return result;
    }
    
    private Set<Integer> getActiveTransactions(final int connectionId) {
        Set<Integer> result = activeTransactionRegistry.get(connectionId);
        if (null == result) {
            throw new IllegalStateException("No transaction ID generator found for connectionId: " + connectionId);
        }
        return result;
    }
    
}
