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

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.database.exception.core.exception.SQLDialectException;
import org.apache.shardingsphere.database.protocol.firebird.err.FirebirdErrorPacketFactory;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.FirebirdInfoPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.FirebirdInfoPacketType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.type.common.FirebirdCommonInfoPacketType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.type.sql.FirebirdSQLInfoPacketType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.type.sql.FirebirdSQLInfoReturnPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.type.sql.FirebirdSQLRecordsInfo;
import org.apache.shardingsphere.database.protocol.firebird.packet.generic.FirebirdGenericResponsePacket;
import org.apache.shardingsphere.database.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.engine.SQLBindEngine;
import org.apache.shardingsphere.infra.connection.kernel.KernelProcessor;
import org.apache.shardingsphere.infra.exception.external.sql.ShardingSphereSQLException;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.ConnectionMode;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.session.query.QueryContext;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.command.executor.CommandExecutor;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.FirebirdServerPreparedStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.DeleteStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.InsertStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.UpdateStatement;
import org.firebirdsql.gds.ISCConstants;
import org.firebirdsql.jdbc.FirebirdPreparedStatement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Database info command executor for Firebird.
 */
@RequiredArgsConstructor
public final class FirebirdSQLInfoExecutor implements CommandExecutor {
    
    private final FirebirdInfoPacket packet;
    
    private final ConnectionSession connectionSession;
    
    @Override
    public Collection<DatabasePacket> execute() {
        String plan = "";
        if (isPlanRequested()) {
            FirebirdServerPreparedStatement statement = connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(packet.getHandle());
            if (null == statement) {
                return Collections.singleton(FirebirdErrorPacketFactory.newInstance(new SQLException("", null, ISCConstants.isc_bad_req_handle)));
            }
            try {
                plan = getExecutionPlan(statement);
            } catch (final SQLException | SQLDialectException | ShardingSphereSQLException ex) {
                return Collections.singleton(FirebirdErrorPacketFactory.newInstance(ex));
            }
        }
        return Collections.singleton(new FirebirdGenericResponsePacket().setData(new FirebirdSQLInfoReturnPacket(packet.getInfoItems(), getRecordsInfo(), plan, packet.getMaxLength())));
    }
    
    @HighFrequencyInvocation
    private boolean isPlanRequested() {
        for (FirebirdInfoPacketType each : packet.getInfoItems()) {
            if (FirebirdCommonInfoPacketType.END == each) {
                return false;
            }
            if (FirebirdSQLInfoPacketType.GET_PLAN == each) {
                return true;
            }
        }
        return false;
    }
    
    @HighFrequencyInvocation
    private String getExecutionPlan(final FirebirdServerPreparedStatement statement) throws SQLException {
        ShardingSphereMetaData metaData = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData();
        SQLStatementContext sqlStatementContext = new SQLBindEngine(metaData, connectionSession.getCurrentDatabaseName(), statement.getHintValueContext())
                .bind(statement.getSqlStatementContext().getSqlStatement());
        QueryContext queryContext = QueryContext.forExecutionPlan(sqlStatementContext, statement.getSql(), statement.getHintValueContext(), connectionSession.getConnectionContext(), metaData);
        Collection<ExecutionUnit> units = new KernelProcessor().generateExecutionContext(queryContext, metaData.getGlobalRuleMetaData(), metaData.getProps()).getExecutionUnits();
        Collection<String> plans = new ArrayList<>(units.size());
        for (ExecutionUnit each : units) {
            Connection connection = connectionSession.getDatabaseConnectionManager().getConnections(queryContext.getUsedDatabase().getName(), each.getDataSourceName(), 0, 1,
                    ConnectionMode.MEMORY_STRICTLY).get(0);
            try (PreparedStatement physicalStatement = connection.prepareStatement(each.getSqlUnit().getSql())) {
                String plan = physicalStatement.unwrap(FirebirdPreparedStatement.class).getExecutionPlan();
                if (null != plan && !plan.isEmpty()) {
                    plans.add(plan);
                }
            }
        }
        return String.join("\n", plans);
    }
    
    private FirebirdSQLRecordsInfo getRecordsInfo() {
        FirebirdServerPreparedStatement preparedStatement = connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(packet.getHandle());
        if (null == preparedStatement) {
            return new FirebirdSQLRecordsInfo(0L, 0L, 0L);
        }
        long affectedRows = preparedStatement.getAffectedRows();
        SQLStatement sqlStatement = preparedStatement.getSqlStatementContext().getSqlStatement();
        long insertCount = sqlStatement instanceof InsertStatement ? affectedRows : 0L;
        long updateCount = sqlStatement instanceof UpdateStatement ? affectedRows : 0L;
        long deleteCount = sqlStatement instanceof DeleteStatement ? affectedRows : 0L;
        return new FirebirdSQLRecordsInfo(insertCount, updateCount, deleteCount);
    }
}
