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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidSegstrIdException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidStatementHandleException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidTransactionHandleException;
import org.apache.shardingsphere.database.protocol.binary.BinaryRow;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.FirebirdBinaryColumnType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.execute.FirebirdExecuteStatementPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.execute.protocol.FirebirdBlobBinaryProtocolValue;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.prepare.FirebirdReturnColumnPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.generic.FirebirdGenericResponsePacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.generic.FirebirdSQLResponsePacket;
import org.apache.shardingsphere.database.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.infra.binder.context.aware.ParameterAware;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.session.query.QueryContext;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.handler.ProxyBackendHandler;
import org.apache.shardingsphere.proxy.backend.handler.ProxyBackendHandlerFactory;
import org.apache.shardingsphere.proxy.backend.response.data.QueryResponseRow;
import org.apache.shardingsphere.proxy.backend.response.header.ResponseHeader;
import org.apache.shardingsphere.proxy.backend.response.header.query.QueryResponseHeader;
import org.apache.shardingsphere.proxy.backend.response.header.update.UpdateResponseHeader;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.command.executor.CommandExecutor;
import org.apache.shardingsphere.proxy.frontend.command.executor.ResponseType;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.FirebirdServerPreparedStatement;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.cache.FirebirdBlobWriteCache;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.statement.FirebirdBinaryRowBuilder;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.statement.FirebirdStatementResourceCleaner;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.statement.fetch.FirebirdFetchStatementCache;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.transaction.FirebirdTransactionIdGenerator;
import org.firebirdsql.encodings.EncodingDefinition;
import org.firebirdsql.encodings.EncodingFactory;
import org.firebirdsql.gds.ISCConstants;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Firebird execute statement command executor.
 */
@RequiredArgsConstructor
public final class FirebirdExecuteStatementCommandExecutor implements CommandExecutor {
    
    private final FirebirdExecuteStatementPacket packet;
    
    private final ConnectionSession connectionSession;
    
    private ProxyBackendHandler proxyBackendHandler;
    
    @Getter
    private ResponseType responseType;
    
    @Override
    public Collection<DatabasePacket> execute() throws SQLException {
        connectionSession.beginPreparedStatementCache(FirebirdStatementResourceCleaner.createPreparedStatementCacheKey(packet.getStatementId()));
        try {
            FirebirdServerPreparedStatement preparedStatement = connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(packet.getStatementId());
            if (null == preparedStatement) {
                throw new InvalidStatementHandleException(packet.getStatementId());
            }
            validateTransactionHandle();
            ResponseHeader responseHeader = executePreparedStatement(preparedStatement, packet.getParameterValues());
            if (responseHeader instanceof QueryResponseHeader) {
                responseType = ResponseType.QUERY;
                FirebirdFetchStatementCache.getInstance().registerStatement(connectionSession.getConnectionId(), packet.getStatementId(), proxyBackendHandler);
                connectionSession.getDatabaseConnectionManager().markResourceInUse(proxyBackendHandler);
            } else {
                responseType = ResponseType.UPDATE;
                preparedStatement.setAffectedRows(((UpdateResponseHeader) responseHeader).getUpdateCount());
            }
            Collection<DatabasePacket> result = new LinkedList<>();
            if (packet.isStoredProcedure() && proxyBackendHandler.next()) {
                result.add(getSQLResponse());
            }
            result.add(new FirebirdGenericResponsePacket());
            return result;
        } finally {
            connectionSession.finishPreparedStatementCache();
        }
    }
    
    private void validateTransactionHandle() {
        ShardingSpherePreconditions.checkState(FirebirdTransactionIdGenerator.getInstance().isTransactionActive(connectionSession.getConnectionId(), packet.getTransactionId()),
                () -> new InvalidTransactionHandleException(packet.getTransactionId()));
    }
    
    private ResponseHeader executePreparedStatement(final FirebirdServerPreparedStatement preparedStatement, final List<Object> params) throws SQLException {
        List<Long> blobIdsToRemove = bindBlobParameters(preparedStatement.getParameterColumns(), params);
        try {
            SQLStatementContext sqlStatementContext = preparedStatement.getSqlStatementContext();
            if (sqlStatementContext instanceof ParameterAware) {
                ((ParameterAware) sqlStatementContext).bindParameters(params);
            }
            QueryContext queryContext = new QueryContext(sqlStatementContext, preparedStatement.getSql(), params, preparedStatement.getHintValueContext(), connectionSession.getConnectionContext(),
                    ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData(), true);
            proxyBackendHandler = ProxyBackendHandlerFactory.newInstance(TypedSPILoader.getService(DatabaseType.class, "Firebird"), queryContext, connectionSession, true);
            return proxyBackendHandler.execute();
        } finally {
            clearBlobUploads(blobIdsToRemove);
        }
    }
    
    private List<Long> bindBlobParameters(final List<FirebirdReturnColumnPacket> parameterColumns, final List<Object> params) {
        List<FirebirdBinaryColumnType> parameterTypes = packet.getParameterTypes();
        List<Long> blobIds = new LinkedList<>();
        int paramCount = Math.min(parameterTypes.size(), params.size());
        for (int i = 0; i < paramCount; i++) {
            if (parameterTypes.get(i) != FirebirdBinaryColumnType.BLOB) {
                continue;
            }
            Object paramValue = params.get(i);
            if (!(paramValue instanceof Long)) {
                params.set(i, null);
                continue;
            }
            long blobId = (Long) paramValue;
            if (0L == blobId) {
                params.set(i, new byte[0]);
                continue;
            }
            if (blobId < 0L) {
                byte[] resultBlobContent = FirebirdBlobBinaryProtocolValue.getBlobContent(connectionSession.getConnectionId(), blobId);
                ShardingSpherePreconditions.checkNotNull(resultBlobContent, () -> new InvalidSegstrIdException(blobId));
                params.set(i, resultBlobContent);
                continue;
            }
            ShardingSpherePreconditions.checkState(FirebirdBlobWriteCache.getInstance().isClosed(connectionSession.getConnectionId(), blobId), () -> new InvalidSegstrIdException(blobId));
            Optional<byte[]> blobData = FirebirdBlobWriteCache.getInstance().getBlobData(connectionSession.getConnectionId(), blobId);
            params.set(i, toBlobParameterValue(parameterColumns, i, blobData.get()));
            blobIds.add(blobId);
        }
        return blobIds;
    }
    
    private Object toBlobParameterValue(final List<FirebirdReturnColumnPacket> parameterColumns, final int index, final byte[] content) {
        Optional<Charset> charset = findTextBlobCharset(parameterColumns, index);
        if (!charset.isPresent()) {
            return content;
        }
        try {
            return charset.get().newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(content)).toString();
        } catch (final CharacterCodingException ignored) {
            return content;
        }
    }
    
    private Optional<Charset> findTextBlobCharset(final List<FirebirdReturnColumnPacket> parameterColumns, final int index) {
        if (index >= parameterColumns.size()) {
            return Optional.empty();
        }
        FirebirdReturnColumnPacket parameterColumn = parameterColumns.get(index);
        if (FirebirdBinaryColumnType.BLOB != parameterColumn.getColumnType() || !Integer.valueOf(ISCConstants.BLOB_SUB_TYPE_TEXT).equals(parameterColumn.getBlobSubType())
                || ISCConstants.CS_NONE == parameterColumn.getBlobCharsetId() || ISCConstants.CS_BINARY == parameterColumn.getBlobCharsetId()) {
            return Optional.empty();
        }
        EncodingDefinition encodingDefinition = EncodingFactory.getPlatformDefault().getEncodingDefinitionByCharacterSetId(parameterColumn.getBlobCharsetId());
        return null == encodingDefinition || encodingDefinition.isInformationOnly() ? Optional.empty() : Optional.ofNullable(encodingDefinition.getJavaCharset());
    }
    
    private void clearBlobUploads(final List<Long> blobIds) {
        for (Long each : blobIds) {
            FirebirdBlobWriteCache.getInstance().removeWrite(connectionSession.getConnectionId(), each);
        }
    }
    
    private FirebirdSQLResponsePacket getSQLResponse() throws SQLException {
        QueryResponseRow queryResponseRow = proxyBackendHandler.getRowData();
        BinaryRow row = FirebirdBinaryRowBuilder.build(queryResponseRow);
        return new FirebirdSQLResponsePacket(row);
    }
}
