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

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.execute.FirebirdExecuteImmediatePacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.generic.FirebirdGenericResponsePacket;
import org.apache.shardingsphere.database.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.proxy.backend.handler.ProxyBackendHandlerFactory;
import org.apache.shardingsphere.proxy.backend.handler.ProxySQLComQueryParser;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.command.executor.CommandExecutor;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.transaction.FirebirdTransactionIdGenerator;
import org.apache.shardingsphere.sql.parser.statement.core.enums.TransactionAccessType;
import org.apache.shardingsphere.sql.parser.statement.core.enums.TransactionIsolationLevel;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.tcl.SetTransactionStatement;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

/**
 * Firebird execute immediate command executor.
 */
@RequiredArgsConstructor
public final class FirebirdExecuteImmediateCommandExecutor implements CommandExecutor {
    
    private final FirebirdExecuteImmediatePacket packet;
    
    private final ConnectionSession connectionSession;
    
    @Override
    public Collection<DatabasePacket> execute() throws SQLException {
        DatabaseType databaseType = TypedSPILoader.getService(DatabaseType.class, "Firebird");
        SQLStatement sqlStatement = ProxySQLComQueryParser.parse(packet.getSQL(), databaseType, connectionSession);
        int transactionId = sqlStatement instanceof SetTransactionStatement ? startTransaction((SetTransactionStatement) sqlStatement) : executeInTransaction(databaseType, sqlStatement);
        return Collections.singleton(new FirebirdGenericResponsePacket().setHandle(transactionId));
    }
    
    private int startTransaction(final SetTransactionStatement sqlStatement) {
        if (!connectionSession.getTransactionStatus().isInTransaction()) {
            connectionSession.setAutoCommit(false);
            connectionSession.setReadOnly(sqlStatement.isDesiredAccessMode(TransactionAccessType.READ_ONLY));
            connectionSession.setIsolationLevel(sqlStatement.getIsolationLevel().orElse(TransactionIsolationLevel.REPEATABLE_READ));
        }
        return FirebirdTransactionIdGenerator.getInstance().nextTransactionId(connectionSession.getConnectionId());
    }
    
    private int executeInTransaction(final DatabaseType databaseType, final SQLStatement sqlStatement) throws SQLException {
        FirebirdTransactionIdGenerator.getInstance().checkTransactionForStatement(connectionSession, packet.getTransactionId());
        ProxyBackendHandlerFactory.newInstance(databaseType, packet.getSQL(), sqlStatement, connectionSession, packet.getHintValueContext()).execute();
        FirebirdTransactionIdGenerator.getInstance().updateBackendTransactionOwner(connectionSession, packet.getTransactionId());
        return FirebirdTransactionIdGenerator.getInstance().closeTransactionEndedBy(connectionSession.getConnectionId(), packet.getTransactionId(), sqlStatement);
    }
}
