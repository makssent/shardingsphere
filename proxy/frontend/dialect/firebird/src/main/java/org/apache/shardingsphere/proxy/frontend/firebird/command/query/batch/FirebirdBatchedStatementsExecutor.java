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

import com.google.common.primitives.Ints;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.database.connector.firebird.metadata.data.FirebirdTypeMetadataRegistry;
import org.apache.shardingsphere.database.exception.firebird.exception.FirebirdException;
import org.apache.shardingsphere.database.exception.firebird.exception.FirebirdException.StatusVectorEntry;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.DateTimeOutOfRangeException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.NumericOutOfRangeException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.ParameterConversionException;
import org.apache.shardingsphere.database.protocol.firebird.err.FirebirdStatusVector;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.prepare.FirebirdReturnColumnPacket;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.aware.ParameterAware;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.engine.SQLBindEngine;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.connection.kernel.KernelProcessor;
import org.apache.shardingsphere.infra.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.executor.audit.SQLAuditEngine;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroup;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroupContext;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroupReportContext;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionContext;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.context.SQLUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.ConnectionMode;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.SQLExecutorExceptionHandler;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutor;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutorCallback;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.DriverExecutionPrepareEngine;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.jdbc.JDBCDriverType;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.jdbc.StatementOption;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.resource.ResourceMetaData;
import org.apache.shardingsphere.infra.metadata.database.resource.unit.StorageUnit;
import org.apache.shardingsphere.infra.route.context.RouteMapper;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.session.query.QueryContext;
import org.apache.shardingsphere.mode.metadata.MetaDataContexts;
import org.apache.shardingsphere.parser.rule.SQLParserRule;
import org.apache.shardingsphere.proxy.backend.connector.jdbc.statement.JDBCBackendStatement;
import org.apache.shardingsphere.proxy.backend.context.BackendExecutorContext;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.FirebirdServerPreparedStatement;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.FirebirdBlobParameterConverter;
import org.apache.shardingsphere.sql.parser.engine.core.database.parser.SQLParserExecutor;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;
import org.firebirdsql.encodings.EncodingFactory;
import org.firebirdsql.extern.decimal.Decimal128;
import org.firebirdsql.extern.decimal.Decimal64;
import org.firebirdsql.gds.ISCConstants;
import org.firebirdsql.jdbc.JaybirdTypeCodes;
import org.firebirdsql.jdbc.field.TypeConversionException;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Batched statements executor for Firebird.
 */
public final class FirebirdBatchedStatementsExecutor {
    
    private static final int NOT_EXECUTED = Integer.MIN_VALUE;
    
    private final KernelProcessor kernelProcessor = new KernelProcessor();
    
    private final JDBCExecutor jdbcExecutor;
    
    private final ConnectionSession connectionSession;
    
    private final MetaDataContexts metaDataContexts;
    
    private final FirebirdServerPreparedStatement preparedStatement;
    
    private final Map<ExecutionUnit, List<List<Object>>> executionUnitParams = new LinkedHashMap<>();
    
    private final Map<ExecutionUnit, List<Integer>> executionUnitBatchMessageIndexes = new LinkedHashMap<>();
    
    private final Map<PreparedStatement, List<List<Object>>> pendingBatchParams = new IdentityHashMap<>();
    
    private final Map<Connection, Map<String, PreparedStatement>> parameterConversions;
    
    private final Map<PreparedStatement, ParameterMetaData> parameterMetaDataCache;
    
    private final Map<Connection, Boolean> firebirdConnections;
    
    private final Map<String, Map<List<Integer>, String>> textBlobSqlVariants = new HashMap<>();
    
    private final int batchMessageCount;
    
    private final boolean multiError;
    
    private final boolean preserveMessageOrder;
    
    private final Map<?, RawTextBlob> rawTextBlobs;
    
    private final ExecutionContext anyExecutionContext;
    
    private ExecutionGroupContext<JDBCExecutionUnit> executionGroupContext;
    
    public FirebirdBatchedStatementsExecutor(final ConnectionSession connectionSession, final FirebirdServerPreparedStatement preparedStatement, final List<List<Object>> parameterSets,
                                             final boolean multiError) {
        this(connectionSession, preparedStatement, parameterSets, multiError, Collections.emptyMap());
    }
    
    FirebirdBatchedStatementsExecutor(final ConnectionSession connectionSession, final FirebirdServerPreparedStatement preparedStatement, final List<List<Object>> parameterSets,
                                      final boolean multiError, final Map<?, RawTextBlob> rawTextBlobs) {
        this(connectionSession, preparedStatement, parameterSets, multiError, rawTextBlobs, false);
    }
    
    FirebirdBatchedStatementsExecutor(final ConnectionSession connectionSession, final FirebirdServerPreparedStatement preparedStatement, final List<List<Object>> parameterSets,
                                      final boolean multiError, final Map<?, RawTextBlob> rawTextBlobs, final boolean preserveMessageOrder) {
        jdbcExecutor = new JDBCExecutor(BackendExecutorContext.getInstance().getExecutorEngine(), connectionSession.getConnectionContext());
        this.multiError = multiError;
        this.preserveMessageOrder = preserveMessageOrder;
        parameterConversions = preserveMessageOrder ? new IdentityHashMap<>(1) : Collections.emptyMap();
        parameterMetaDataCache = preserveMessageOrder ? new IdentityHashMap<>(1) : Collections.emptyMap();
        firebirdConnections = preserveMessageOrder ? new IdentityHashMap<>(1) : Collections.emptyMap();
        this.rawTextBlobs = rawTextBlobs;
        this.connectionSession = connectionSession;
        metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
        this.preparedStatement = preparedStatement;
        batchMessageCount = parameterSets.size();
        Iterator<List<Object>> parameterSetsIterator = parameterSets.iterator();
        SQLStatementContext sqlStatementContext = null;
        ExecutionContext executionContext = null;
        int batchMessageIndex = 0;
        if (parameterSetsIterator.hasNext()) {
            List<Object> firstGroupOfParam = parameterSetsIterator.next();
            sqlStatementContext = createSQLStatementContext(firstGroupOfParam);
            executionContext = createExecutionContext(createQueryContext(sqlStatementContext, firstGroupOfParam));
            addExecutionUnitParams(executionContext, batchMessageIndex++);
        }
        anyExecutionContext = executionContext;
        prepareForRestOfParametersSet(parameterSetsIterator, sqlStatementContext, batchMessageIndex);
    }
    
    private SQLStatementContext createSQLStatementContext(final List<Object> params) {
        SQLStatementContext result = new SQLBindEngine(metaDataContexts.getMetaData(), connectionSession.getCurrentDatabaseName(), preparedStatement.getHintValueContext())
                .bind(preparedStatement.getSqlStatementContext().getSqlStatement());
        if (result instanceof ParameterAware) {
            ((ParameterAware) result).bindParameters(params);
        }
        return result;
    }
    
    private void prepareForRestOfParametersSet(final Iterator<List<Object>> paramSetsIterator, final SQLStatementContext sqlStatementContext, final int firstBatchMessageIndex) {
        int batchMessageIndex = firstBatchMessageIndex;
        while (paramSetsIterator.hasNext()) {
            List<Object> eachGroupOfParam = paramSetsIterator.next();
            if (sqlStatementContext instanceof ParameterAware) {
                ((ParameterAware) sqlStatementContext).bindParameters(eachGroupOfParam);
            }
            ExecutionContext eachExecutionContext = createExecutionContext(createQueryContext(sqlStatementContext, eachGroupOfParam));
            addExecutionUnitParams(eachExecutionContext, batchMessageIndex++);
        }
    }
    
    @HighFrequencyInvocation
    private void addExecutionUnitParams(final ExecutionContext executionContext, final int batchMessageIndex) {
        for (ExecutionUnit each : executionContext.getExecutionUnits()) {
            ExecutionUnit executionUnit = rewriteTextBlobParameters(each);
            executionUnitParams.computeIfAbsent(executionUnit, key -> new LinkedList<>()).add(executionUnit.getSqlUnit().getParameters());
            executionUnitBatchMessageIndexes.computeIfAbsent(executionUnit, key -> new LinkedList<>()).add(batchMessageIndex);
        }
    }
    
    @HighFrequencyInvocation
    private ExecutionUnit rewriteTextBlobParameters(final ExecutionUnit executionUnit) {
        SQLUnit sqlUnit = executionUnit.getSqlUnit();
        if (rawTextBlobs.isEmpty() || !containsTextBlob(sqlUnit.getParameters())) {
            return executionUnit;
        }
        Map<String, StorageUnit> storageUnits = metaDataContexts.getMetaData().getDatabase(connectionSession.getUsedDatabaseName()).getResourceMetaData().getStorageUnits();
        StorageUnit storageUnit = storageUnits.get(executionUnit.getDataSourceName());
        DatabaseType storageType = null == storageUnit ? storageUnits.values().iterator().next().getStorageType() : storageUnit.getStorageType();
        if (!"Firebird".equals(storageType.getType())) {
            return executionUnit;
        }
        String sql = sqlUnit.getSql();
        List<Object> params = new ArrayList<>(sqlUnit.getParameters());
        List<Integer> charsets = new ArrayList<>(params.size());
        boolean rewriteSql = false;
        for (int i = 0; i < params.size(); i++) {
            RawTextBlob blob = rawTextBlobs.get(params.get(i));
            charsets.add(null == blob || null != blob.parameter ? -1 : blob.charsetId);
            if (null != blob) {
                if (null == blob.parameter) {
                    params.set(i, blob.content);
                    rewriteSql = true;
                } else {
                    params.set(i, getTextColumnValue(sqlUnit, storageUnit, blob, params.get(i)));
                }
            }
        }
        if (!rewriteSql) {
            return new ExecutionUnit(executionUnit.getDataSourceName(), new SQLUnit(sql, params, sqlUnit.getTableRouteMappers()));
        }
        long cacheSize = metaDataContexts.getMetaData().getGlobalRuleMetaData().getSingleRule(SQLParserRule.class).getParseTreeCache().getMaximumSize();
        List<Integer> markers = preparedStatement.getPhysicalParameterMarkers(sql, cacheSize, key -> getParameterMarkers(key, storageType));
        ShardingSpherePreconditions.checkState(markers.size() == params.size(), () -> new IllegalStateException("Physical SQL parameter count does not match its values."));
        String rewritten = textBlobSqlVariants.computeIfAbsent(sql, key -> new HashMap<>()).computeIfAbsent(charsets, key -> rewriteTextBlobSql(sql, markers, key));
        return new ExecutionUnit(executionUnit.getDataSourceName(), new SQLUnit(rewritten, params, sqlUnit.getTableRouteMappers()));
    }
    
    @HighFrequencyInvocation
    private boolean containsTextBlob(final List<Object> params) {
        for (Object each : params) {
            if (rawTextBlobs.containsKey(each)) {
                return true;
            }
        }
        return false;
    }
    
    @HighFrequencyInvocation
    private Object getTextColumnValue(final SQLUnit sqlUnit, @Nullable final StorageUnit storageUnit, final RawTextBlob blob, final Object value) {
        Optional<Integer> charsetId = blob.parameter.findTargetCharacterSetId();
        if (null != storageUnit && blob.parameter.findTableName().isPresent() && blob.parameter.findColumnName().isPresent()) {
            String tableName = blob.parameter.findTableName().get();
            for (RouteMapper each : sqlUnit.getTableRouteMappers()) {
                if (each.getLogicName().equalsIgnoreCase(tableName)) {
                    tableName = each.getActualName();
                    break;
                }
            }
            Optional<Integer> physicalCharset = FirebirdTypeMetadataRegistry.findColumnType(storageUnit.getDataSource(), tableName, blob.parameter.findColumnName().get())
                    .map(optional -> optional.getCharacterSetId());
            if (physicalCharset.isPresent()) {
                charsetId = physicalCharset;
            }
        }
        return charsetId.isPresent() && charsetId.get() <= 1 ? blob.content : value;
    }
    
    private List<Integer> getParameterMarkers(final String sql, final DatabaseType storageType) {
        List<Integer> result = new ArrayList<>();
        collectParameterMarkers(new SQLParserExecutor(storageType).parse(sql).getRootNode(), result);
        return result;
    }
    
    private void collectParameterMarkers(final ParseTree node, final List<Integer> markers) {
        if (node instanceof TerminalNode && "?".equals(node.getText())) {
            markers.add(((TerminalNode) node).getSymbol().getStartIndex());
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            collectParameterMarkers(node.getChild(i), markers);
        }
    }
    
    @HighFrequencyInvocation
    private String rewriteTextBlobSql(final String sql, final List<Integer> markers, final List<Integer> charsets) {
        StringBuilder rewritten = new StringBuilder(sql);
        for (int i = markers.size() - 1; i >= 0; i--) {
            if (charsets.get(i) >= 0) {
                String charsetName = EncodingFactory.getPlatformDefault().getEncodingDefinitionByCharacterSetId(charsets.get(i)).getFirebirdEncodingName();
                String replacement = new StringBuilder("CAST(CAST(? AS BLOB SUB_TYPE TEXT CHARACTER SET NONE) AS BLOB SUB_TYPE TEXT CHARACTER SET ").append(charsetName).append(')').toString();
                rewritten.replace(markers.get(i), markers.get(i) + 1, replacement);
            }
        }
        return rewritten.toString();
    }
    
    private ExecutionContext createExecutionContext(final QueryContext queryContext) {
        ShardingSphereDatabase currentDatabase = metaDataContexts.getMetaData().getDatabase(connectionSession.getCurrentDatabaseName());
        SQLAuditEngine.audit(queryContext, currentDatabase);
        return kernelProcessor.generateExecutionContext(queryContext, metaDataContexts.getMetaData().getGlobalRuleMetaData(), metaDataContexts.getMetaData().getProps());
    }
    
    private QueryContext createQueryContext(final SQLStatementContext sqlStatementContext, final List<Object> params) {
        return new QueryContext(sqlStatementContext, preparedStatement.getSql(), params, preparedStatement.getHintValueContext(), connectionSession.getConnectionContext(),
                metaDataContexts.getMetaData());
    }
    
    /**
     * Execute batch.
     *
     * @return batch completion in the Firebird domain
     * @throws SQLException SQL exception
     */
    public FirebirdBatchCompletion executeBatch() throws SQLException {
        connectionSession.getDatabaseConnectionManager().handleAutoCommit();
        prepareBatchedStatements();
        List<BatchExecution> batchExecutions;
        List<BatchExecutionUnitResult> executeResults;
        try {
            batchExecutions = createBatchExecutionPlan();
            executeResults = executeBatchedPreparedStatements(batchExecutions);
        } finally {
            closePreparedStatements();
        }
        return createBatchCompletion(batchExecutions, executeResults);
    }
    
    private void prepareBatchedStatements() throws SQLException {
        Collection<ShardingSphereRule> rules = metaDataContexts.getMetaData().getDatabase(connectionSession.getUsedDatabaseName()).getRuleMetaData().getRules();
        int maxConnectionsSizePerQuery = metaDataContexts.getMetaData().getProps().<Integer>getValue(ConfigurationPropertyKey.MAX_CONNECTIONS_SIZE_PER_QUERY);
        DriverExecutionPrepareEngine<JDBCExecutionUnit, Connection> prepareEngine = new DriverExecutionPrepareEngine<>(JDBCDriverType.PREPARED_STATEMENT, maxConnectionsSizePerQuery,
                connectionSession.getDatabaseConnectionManager(), (JDBCBackendStatement) connectionSession.getStatementManager(), new StatementOption(false), rules, metaDataContexts.getMetaData());
        executionGroupContext = prepareEngine.prepare(connectionSession.getUsedDatabaseName(), anyExecutionContext, executionUnitParams.keySet(),
                new ExecutionGroupReportContext(connectionSession.getProcessId(), connectionSession.getUsedDatabaseName(), connectionSession.getConnectionContext().getGrantee()));
    }
    
    private void addPendingBatch(final BatchExecution batchExecution) throws SQLException {
        PreparedStatement preparedStatement = (PreparedStatement) batchExecution.executionUnit.getStorageResource();
        List<List<Object>> params = getBatchParams(batchExecution);
        pendingBatchParams.put(preparedStatement, params);
        if (!preserveMessageOrder) {
            addBatchedParameters(preparedStatement, params);
            return;
        }
        int addedCount = 0;
        try {
            for (List<Object> each : params) {
                addBatchedParameter(preparedStatement, each);
                addedCount++;
            }
        } catch (final ParameterConversionException ex) {
            pendingBatchParams.put(preparedStatement, params.subList(0, addedCount));
            throw ex;
        }
    }
    
    private void addBatchedParameters(final PreparedStatement preparedStatement, final List<List<Object>> params) throws SQLException {
        for (List<Object> eachGroupParam : params) {
            addBatchedParameter(preparedStatement, eachGroupParam);
        }
    }
    
    private void addBatchedParameter(final PreparedStatement preparedStatement, final List<Object> params) throws SQLException {
        ListIterator<Object> eachParams = params.listIterator();
        while (eachParams.hasNext()) {
            int paramIndex = eachParams.nextIndex() + 1;
            Object value = eachParams.next();
            try {
                if (preserveMessageOrder && value instanceof String) {
                    value = convertBlobParameter(preparedStatement, paramIndex, (String) value);
                }
                preparedStatement.setObject(paramIndex, value);
            } catch (final TypeConversionException ex) {
                if (preserveMessageOrder && value instanceof String) {
                    throw createParameterConversionException(preparedStatement, paramIndex, (String) value, ex);
                }
                throw ex;
            }
        }
        preparedStatement.addBatch();
    }
    
    private Object convertBlobParameter(final PreparedStatement statement, final int index, final String value) throws SQLException {
        ParameterMetaData metadata = getParameterMetaData(statement);
        int jdbcType = metadata.getParameterType(index);
        boolean nativeScalar = Types.BOOLEAN == jdbcType || Types.FLOAT == jdbcType || Types.DOUBLE == jdbcType || JaybirdTypeCodes.DECFLOAT == jdbcType
                || Types.NUMERIC == jdbcType && "INT128".equals(metadata.getParameterTypeName(index));
        if (nativeScalar && !isFirebirdConnection(statement.getConnection())) {
            return value;
        }
        if (Types.BOOLEAN == jdbcType || Types.FLOAT == jdbcType || Types.DOUBLE == jdbcType) {
            String typeName = Types.DOUBLE == jdbcType ? "DOUBLE PRECISION" : JDBCType.valueOf(jdbcType).getName();
            return convertParameterAtServer(statement, jdbcType, typeName, value);
        }
        if (JaybirdTypeCodes.DECFLOAT == jdbcType) {
            String typeName = 16 == metadata.getPrecision(index) ? "DECFLOAT(16)" : "DECFLOAT(34)";
            return convertParameterAtServer(statement, jdbcType, typeName, value);
        }
        if (Types.NUMERIC == jdbcType && "INT128".equals(metadata.getParameterTypeName(index))) {
            return convertParameterAtServer(statement, jdbcType, "INT128", value);
        }
        if (Types.DATE != jdbcType && Types.TIME != jdbcType && Types.TIMESTAMP != jdbcType && Types.TIME_WITH_TIMEZONE != jdbcType && Types.TIMESTAMP_WITH_TIMEZONE != jdbcType) {
            return value;
        }
        int start = 0;
        int end = value.length();
        while (start < end && (' ' == value.charAt(start) || '\t' == value.charAt(start))) {
            start++;
        }
        while (end > start && (' ' == value.charAt(end - 1) || '\t' == value.charAt(end - 1))) {
            end--;
        }
        try {
            if (Types.DATE == jdbcType) {
                return parseTemporalDate(value, start, end);
            }
            if (Types.TIME == jdbcType) {
                return parseTemporalTime(value, start, end);
            }
            if (Types.TIME_WITH_TIMEZONE == jdbcType) {
                return parseOffsetTime(value, start, end);
            }
            if (Types.TIMESTAMP_WITH_TIMEZONE == jdbcType) {
                return convertParameterAtServer(statement, jdbcType, getTemporalTypeName(jdbcType), value);
            }
            int separator = value.indexOf(' ', start);
            return LocalDateTime.of(parseTemporalDate(value, start, separator), parseTemporalTime(value, separator + 1, end));
        } catch (final DateTimeException | NumberFormatException ignored) {
            return convertParameterAtServer(statement, jdbcType, getTemporalTypeName(jdbcType), value);
        }
    }
    
    private ParameterMetaData getParameterMetaData(final PreparedStatement statement) throws SQLException {
        ParameterMetaData result = parameterMetaDataCache.get(statement);
        if (null == result) {
            result = statement.getParameterMetaData();
            parameterMetaDataCache.put(statement, result);
        }
        return result;
    }
    
    private boolean isFirebirdConnection(final Connection connection) throws SQLException {
        Boolean result = firebirdConnections.get(connection);
        if (null == result) {
            result = connection.getMetaData().getURL().startsWith("jdbc:firebirdsql:");
            firebirdConnections.put(connection, result);
        }
        return result;
    }
    
    private Object convertParameterAtServer(final PreparedStatement statement, final int jdbcType, final String typeName, final String value) throws SQLException {
        Connection connection = statement.getConnection();
        Map<String, PreparedStatement> statements = parameterConversions.computeIfAbsent(connection, key -> new HashMap<>());
        try {
            PreparedStatement conversion = statements.get(typeName);
            if (null == conversion) {
                String sql = new StringBuilder("SELECT CAST(CAST(? AS BLOB SUB_TYPE TEXT CHARACTER SET NONE) AS ").append(typeName).append(") FROM RDB$DATABASE").toString();
                conversion = connection.prepareStatement(sql);
                statements.put(typeName, conversion);
            }
            conversion.setBytes(1, value.getBytes(StandardCharsets.ISO_8859_1));
            try (ResultSet resultSet = conversion.executeQuery()) {
                resultSet.next();
                switch (jdbcType) {
                    case Types.DATE:
                        return resultSet.getObject(1, LocalDate.class);
                    case Types.TIME:
                        return resultSet.getObject(1, LocalTime.class);
                    case Types.TIME_WITH_TIMEZONE:
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                        return resultSet.getObject(1, ZonedDateTime.class);
                    case Types.TIMESTAMP:
                        return resultSet.getObject(1, LocalDateTime.class);
                    case JaybirdTypeCodes.DECFLOAT:
                        return "DECFLOAT(16)".equals(typeName) ? resultSet.getObject(1, Decimal64.class) : resultSet.getObject(1, Decimal128.class);
                    default:
                        return resultSet.getObject(1);
                }
            }
        } catch (final SQLException ex) {
            if (ISCConstants.isc_convert_error == ex.getErrorCode()) {
                throw new ParameterConversionException(FirebirdBlobParameterConverter.createConversionError(value));
            }
            if (ISCConstants.isc_date_range_exceeded == ex.getErrorCode() || ISCConstants.isc_time_range_exceeded == ex.getErrorCode()
                    || ISCConstants.isc_datetime_range_exceeded == ex.getErrorCode()) {
                throw new ParameterConversionException(new DateTimeOutOfRangeException(jdbcType));
            }
            if (ISCConstants.isc_numeric_out_of_range == ex.getErrorCode()) {
                throw new ParameterConversionException(new NumericOutOfRangeException());
            }
            if (null != ex.getSQLState() && ex.getSQLState().startsWith("22")) {
                throw createNativeParameterConversionException(ex);
            }
            throw ex;
        }
    }
    
    private ParameterConversionException createNativeParameterConversionException(final SQLException cause) {
        List<FirebirdStatusVector.Segment> segments = new FirebirdStatusVector(cause).getSegments();
        Collection<StatusVectorEntry> statusVector = new ArrayList<>(segments.size());
        for (FirebirdStatusVector.Segment each : segments) {
            statusVector.add(new StatusVectorEntry(each.getGdsCode(), new ArrayList<>(each.getArguments())));
        }
        return new ParameterConversionException(new FirebirdException(cause.getMessage(), cause.getSQLState(), cause.getErrorCode(), statusVector));
    }
    
    private static LocalDate parseTemporalDate(final String value, final int start, final int end) {
        int first = value.indexOf('-', start);
        int second = value.indexOf('-', first + 1);
        ShardingSpherePreconditions.checkState(4 == first - start && second > first + 1 && second - first <= 3 && end > second + 1 && end - second <= 3, NumberFormatException::new);
        int year = getTemporalPart(value, start, first);
        ShardingSpherePreconditions.checkState(year > 0, NumberFormatException::new);
        return LocalDate.of(year, getTemporalPart(value, first + 1, second), getTemporalPart(value, second + 1, end));
    }
    
    private static int getTemporalPart(final String value, final int start, final int end) {
        int result = 0;
        for (int i = start; i < end; i++) {
            int digit = value.charAt(i) - '0';
            ShardingSpherePreconditions.checkState(digit >= 0 && digit <= 9, NumberFormatException::new);
            result = result * 10 + digit;
        }
        return result;
    }
    
    private static LocalTime parseTemporalTime(final String value, final int start, final int end) {
        int first = value.indexOf(':', start);
        int second = value.indexOf(':', first + 1);
        int decimal = value.indexOf('.', second + 1);
        int secondsEnd = decimal < 0 ? end : decimal;
        ShardingSpherePreconditions.checkState(first > start && first - start <= 2 && second > first + 1 && second - first <= 3
                && secondsEnd > second + 1 && secondsEnd - second <= 3 && (decimal < 0 || end > decimal + 1 && end - decimal <= 5), NumberFormatException::new);
        int nanos = 0;
        if (decimal >= 0) {
            nanos = getTemporalPart(value, decimal + 1, end);
            for (int i = end - decimal - 1; i < 9; i++) {
                nanos *= 10;
            }
        }
        return LocalTime.of(getTemporalPart(value, start, first), getTemporalPart(value, first + 1, second), getTemporalPart(value, second + 1, secondsEnd), nanos);
    }
    
    private static OffsetTime parseOffsetTime(final String value, final int start, final int end) {
        int offset = end - 6;
        int decimal = value.indexOf('.', start);
        ShardingSpherePreconditions.checkState(offset > start && ('+' == value.charAt(offset) || '-' == value.charAt(offset)) && ':' == value.charAt(end - 3)
                && (decimal < 0 || offset > decimal + 1 && offset - decimal <= 5), NumberFormatException::new);
        return OffsetTime.parse(value.substring(start, end));
    }
    
    private String getTemporalTypeName(final int jdbcType) {
        switch (jdbcType) {
            case Types.TIME_WITH_TIMEZONE:
                return "TIME WITH TIME ZONE";
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return "TIMESTAMP WITH TIME ZONE";
            default:
                return JDBCType.valueOf(jdbcType).getName();
        }
    }
    
    private ParameterConversionException createParameterConversionException(final PreparedStatement statement, final int index, final String value, final TypeConversionException cause) {
        if (null == cause.getCause() && isNumericOutOfRange(statement, index, value)) {
            return new ParameterConversionException(new NumericOutOfRangeException());
        }
        return new ParameterConversionException(FirebirdBlobParameterConverter.createConversionError(value));
    }
    
    private boolean isNumericOutOfRange(final PreparedStatement statement, final int index, final String value) {
        try {
            new BigDecimal(value.trim());
            int type = getParameterMetaData(statement).getParameterType(index);
            return Types.NUMERIC == type || Types.DECIMAL == type;
        } catch (final NumberFormatException | SQLException ignored) {
            return false;
        }
    }
    
    private List<List<Object>> getBatchParams(final BatchExecution batchExecution) {
        return executionUnitParams.getOrDefault(batchExecution.executionUnit.getExecutionUnit(), Collections.emptyList())
                .subList(batchExecution.fromOffset, batchExecution.toOffset);
    }
    
    private List<Integer> getBatchMessageIndexes(final BatchExecution batchExecution) {
        return executionUnitBatchMessageIndexes.getOrDefault(batchExecution.executionUnit.getExecutionUnit(), Collections.emptyList())
                .subList(batchExecution.fromOffset, batchExecution.toOffset);
    }
    
    private void closePreparedStatements() throws SQLException {
        try {
            for (ExecutionGroup<JDBCExecutionUnit> eachGroup : executionGroupContext.getInputGroups()) {
                for (JDBCExecutionUnit each : eachGroup.getInputs()) {
                    each.getStorageResource().close();
                }
            }
        } finally {
            for (Map<String, PreparedStatement> each : parameterConversions.values()) {
                closeParameterConversions(each.values());
            }
        }
    }
    
    private void closeParameterConversions(final Collection<PreparedStatement> statements) throws SQLException {
        for (PreparedStatement each : statements) {
            each.close();
        }
    }
    
    private List<BatchExecutionUnitResult> executeBatchedPreparedStatements(final List<BatchExecution> batchExecutions) throws SQLException {
        boolean isExceptionThrown = SQLExecutorExceptionHandler.isExceptionThrown();
        ShardingSphereDatabase database = metaDataContexts.getMetaData().getDatabase(connectionSession.getUsedDatabaseName());
        JDBCExecutorCallback<BatchExecutionUnitResult> callback =
                new BatchedStatementsJDBCExecutorCallback(database.getProtocolType(), database.getResourceMetaData(), preparedStatement.getSqlStatementContext().getSqlStatement(), isExceptionThrown);
        return useMessageOrder() ? executeInBatchMessageOrder(batchExecutions, callback) : jdbcExecutor.execute(executionGroupContext, callback);
    }
    
    @HighFrequencyInvocation
    private List<BatchExecutionUnitResult> executeInBatchMessageOrder(final List<BatchExecution> batchExecutions, final JDBCExecutorCallback<BatchExecutionUnitResult> callback) throws SQLException {
        List<BatchExecutionUnitResult> result = new LinkedList<>();
        for (BatchExecution each : batchExecutions) {
            try {
                addPendingBatch(each);
            } catch (final ParameterConversionException ex) {
                PreparedStatement statement = (PreparedStatement) each.executionUnit.getStorageResource();
                if (!pendingBatchParams.get(statement).isEmpty()) {
                    jdbcExecutor.execute(createSingleUnitExecutionGroupContext(each.executionUnit), callback);
                }
                throw ex;
            }
            List<BatchExecutionUnitResult> executeResults = jdbcExecutor.execute(createSingleUnitExecutionGroupContext(each.executionUnit), callback);
            result.addAll(executeResults);
            if (!multiError && hasFailures(executeResults)) {
                break;
            }
        }
        return result;
    }
    
    private boolean hasFailures(final List<BatchExecutionUnitResult> results) {
        for (BatchExecutionUnitResult each : results) {
            if (!each.failures.isEmpty()) {
                return true;
            }
        }
        return false;
    }
    
    private ExecutionGroupContext<JDBCExecutionUnit> createSingleUnitExecutionGroupContext(final JDBCExecutionUnit executionUnit) {
        return new ExecutionGroupContext<>(Collections.singleton(new ExecutionGroup<>(Collections.singletonList(executionUnit))), executionGroupContext.getReportContext());
    }
    
    private FirebirdBatchCompletion createBatchCompletion(final List<BatchExecution> batchExecutions, final List<BatchExecutionUnitResult> executeResults) throws SQLException {
        int[] messageUpdateCounts = new int[batchMessageCount];
        Arrays.fill(messageUpdateCounts, NOT_EXECUTED);
        NavigableMap<Integer, SQLException> failedMessages = new TreeMap<>();
        Iterator<BatchExecution> executions = batchExecutions.iterator();
        Iterator<BatchExecutionUnitResult> results = executeResults.iterator();
        while (executions.hasNext() && results.hasNext()) {
            List<Integer> batchMessageIndexes = getBatchMessageIndexes(executions.next());
            BatchExecutionUnitResult eachResult = results.next();
            mergeUpdateCounts(messageUpdateCounts, batchMessageIndexes, eachResult.updateCounts);
            markFailedMessages(messageUpdateCounts, failedMessages, batchMessageIndexes, eachResult.failures);
        }
        int processedMessageCount = getProcessedMessageCount(messageUpdateCounts, failedMessages);
        return new FirebirdBatchCompletion(processedMessageCount, toFirebirdUpdateCounts(messageUpdateCounts, processedMessageCount), createFailures(failedMessages, processedMessageCount));
    }
    
    private void markFailedMessages(final int[] messageUpdateCounts, final Map<Integer, SQLException> failedMessages,
                                    final List<Integer> batchMessageIndexes, final Map<Integer, SQLException> failedOffsets) {
        for (Entry<Integer, SQLException> each : failedOffsets.entrySet()) {
            markFailedMessage(messageUpdateCounts, failedMessages, getBatchMessageIndex(batchMessageIndexes, each.getKey()), each.getValue());
        }
    }
    
    private int getBatchMessageIndex(final List<Integer> batchMessageIndexes, final int offset) {
        if (batchMessageIndexes.isEmpty()) {
            return 0;
        }
        return offset < batchMessageIndexes.size() ? batchMessageIndexes.get(offset) : batchMessageIndexes.get(batchMessageIndexes.size() - 1);
    }
    
    private void markFailedMessage(final int[] messageUpdateCounts, final Map<Integer, SQLException> failedMessages, final int messageIndex, final SQLException failureCause) {
        messageUpdateCounts[messageIndex] = Statement.EXECUTE_FAILED;
        failedMessages.putIfAbsent(messageIndex, failureCause);
    }
    
    private int getProcessedMessageCount(final int[] messageUpdateCounts, final NavigableMap<Integer, SQLException> failedMessages) throws SQLException {
        int notExecutedMessageIndex = batchMessageCount;
        for (int i = 0; i < batchMessageCount; i++) {
            if (NOT_EXECUTED == messageUpdateCounts[i]) {
                notExecutedMessageIndex = i;
                break;
            }
        }
        if (failedMessages.isEmpty()) {
            return notExecutedMessageIndex;
        }
        Entry<Integer, SQLException> firstFailedMessage = failedMessages.firstEntry();
        if (notExecutedMessageIndex <= firstFailedMessage.getKey()) {
            throw firstFailedMessage.getValue();
        }
        return multiError ? notExecutedMessageIndex : Math.min(notExecutedMessageIndex, firstFailedMessage.getKey() + 1);
    }
    
    private Collection<FirebirdBatchCompletion.Failure> createFailures(final Map<Integer, SQLException> failedMessages, final int processedMessageCount) {
        Collection<FirebirdBatchCompletion.Failure> result = new LinkedList<>();
        for (Entry<Integer, SQLException> each : failedMessages.entrySet()) {
            if (each.getKey() < processedMessageCount) {
                result.add(new FirebirdBatchCompletion.Failure(each.getKey(), each.getValue()));
            }
        }
        return result;
    }
    
    private int[] toFirebirdUpdateCounts(final int[] messageUpdateCounts, final int processedMessageCount) {
        int[] result = Arrays.copyOf(messageUpdateCounts, processedMessageCount);
        for (int i = 0; i < result.length; i++) {
            if (Statement.EXECUTE_FAILED == result[i]) {
                result[i] = FirebirdBatchCompletion.EXECUTE_FAILED;
            }
        }
        return result;
    }
    
    private List<BatchExecution> createBatchExecutionPlan() throws SQLException {
        List<JDBCExecutionUnit> executionUnits = new LinkedList<>();
        for (ExecutionGroup<JDBCExecutionUnit> eachGroup : executionGroupContext.getInputGroups()) {
            executionUnits.addAll(eachGroup.getInputs());
        }
        return useMessageOrder() ? createBatchExecutionsInMessageOrder(executionUnits) : createWholeUnitBatchExecutions(executionUnits);
    }
    
    private boolean useMessageOrder() {
        if (!multiError || preserveMessageOrder) {
            return true;
        }
        if (textBlobSqlVariants.isEmpty()) {
            return false;
        }
        Set<String> dataSources = new HashSet<>();
        for (ExecutionUnit each : executionUnitParams.keySet()) {
            if (!dataSources.add(each.getDataSourceName())) {
                return true;
            }
        }
        return false;
    }
    
    private List<BatchExecution> createWholeUnitBatchExecutions(final Collection<JDBCExecutionUnit> executionUnits) throws SQLException {
        List<BatchExecution> result = new ArrayList<>(executionUnits.size());
        for (JDBCExecutionUnit each : executionUnits) {
            BatchExecution batchExecution = new BatchExecution(each, 0, executionUnitParams.getOrDefault(each.getExecutionUnit(), Collections.emptyList()).size());
            addPendingBatch(batchExecution);
            result.add(batchExecution);
        }
        return result;
    }
    
    private List<BatchExecution> createBatchExecutionsInMessageOrder(final Collection<JDBCExecutionUnit> executionUnits) {
        Map<Integer, Collection<JDBCExecutionUnit>> unitsByBatchMessageIndex = new TreeMap<>();
        for (JDBCExecutionUnit each : executionUnits) {
            for (int eachMessageIndex : executionUnitBatchMessageIndexes.getOrDefault(each.getExecutionUnit(), Collections.emptyList())) {
                unitsByBatchMessageIndex.computeIfAbsent(eachMessageIndex, key -> new LinkedList<>()).add(each);
            }
        }
        List<BatchExecution> result = new ArrayList<>(batchMessageCount);
        Map<JDBCExecutionUnit, Integer> consumedOffsets = new IdentityHashMap<>();
        for (Entry<Integer, Collection<JDBCExecutionUnit>> entry : unitsByBatchMessageIndex.entrySet()) {
            for (JDBCExecutionUnit each : entry.getValue()) {
                int offset = consumedOffsets.merge(each, 1, Integer::sum) - 1;
                addInMessageOrder(result, each, offset);
            }
        }
        return result;
    }
    
    private void addInMessageOrder(final List<BatchExecution> batchExecutions, final JDBCExecutionUnit executionUnit, final int offset) {
        if (multiError && !batchExecutions.isEmpty()) {
            BatchExecution previous = batchExecutions.get(batchExecutions.size() - 1);
            if (previous.executionUnit == executionUnit && previous.toOffset == offset) {
                batchExecutions.set(batchExecutions.size() - 1, new BatchExecution(executionUnit, previous.fromOffset, offset + 1));
                return;
            }
        }
        batchExecutions.add(new BatchExecution(executionUnit, offset, offset + 1));
    }
    
    private void mergeUpdateCounts(final int[] result, final List<Integer> batchMessageIndexes, final int[] updateCounts) {
        int countSize = Math.min(batchMessageIndexes.size(), updateCounts.length);
        for (int i = 0; i < countSize; i++) {
            int batchMessageIndex = batchMessageIndexes.get(i);
            result[batchMessageIndex] = mergeUpdateCount(result[batchMessageIndex], updateCounts[i]);
        }
    }
    
    private int mergeUpdateCount(final int current, final int updateCount) {
        if (NOT_EXECUTED == current) {
            return updateCount;
        }
        if (Statement.EXECUTE_FAILED == updateCount || Statement.EXECUTE_FAILED == current) {
            return Statement.EXECUTE_FAILED;
        }
        if (Statement.SUCCESS_NO_INFO == updateCount || Statement.SUCCESS_NO_INFO == current) {
            return Statement.SUCCESS_NO_INFO;
        }
        return current + updateCount;
    }
    
    private final class BatchedStatementsJDBCExecutorCallback extends JDBCExecutorCallback<BatchExecutionUnitResult> {
        
        private BatchedStatementsJDBCExecutorCallback(final DatabaseType protocolType, final ResourceMetaData resourceMetaData, final SQLStatement sqlStatement, final boolean isExceptionThrown) {
            super(protocolType, resourceMetaData, sqlStatement, isExceptionThrown);
        }
        
        @Override
        protected BatchExecutionUnitResult executeSQL(final String sql, final Statement statement, final ConnectionMode connectionMode, final DatabaseType storageType) throws SQLException {
            return executeBatchedStatement((PreparedStatement) statement);
        }
        
        private BatchExecutionUnitResult executeBatchedStatement(final PreparedStatement statement) throws SQLException {
            List<List<Object>> params = pendingBatchParams.getOrDefault(statement, Collections.emptyList());
            List<Integer> updateCounts = new LinkedList<>();
            Map<Integer, SQLException> failures = new TreeMap<>();
            while (true) {
                int executedCount = updateCounts.size();
                try {
                    updateCounts.addAll(Ints.asList(statement.executeBatch()));
                    return new BatchExecutionUnitResult(Ints.toArray(updateCounts), failures);
                } catch (final SQLException ex) {
                    int[] executedUpdateCounts = getExecutedUpdateCounts(ex);
                    updateCounts.addAll(Ints.asList(executedUpdateCounts));
                    registerFailures(failures, ex, updateCounts, executedCount, params.size() - executedCount > executedUpdateCounts.length);
                }
                if (!multiError || updateCounts.size() >= params.size()) {
                    return new BatchExecutionUnitResult(Ints.toArray(updateCounts), failures);
                }
                statement.clearBatch();
                addBatchedParameters(statement, params.subList(updateCounts.size(), params.size()));
            }
        }
        
        private int[] getExecutedUpdateCounts(final SQLException ex) {
            if (!(ex instanceof BatchUpdateException)) {
                return new int[0];
            }
            int[] result = ((BatchUpdateException) ex).getUpdateCounts();
            return null == result ? new int[0] : result;
        }
        
        private void registerFailures(final Map<Integer, SQLException> failures, final SQLException ex, final List<Integer> updateCounts,
                                      final int executedCount, final boolean isHaltedBeforeRemainingMessages) {
            List<Integer> failedOffsets = new LinkedList<>();
            for (int i = executedCount; i < updateCounts.size(); i++) {
                if (Statement.EXECUTE_FAILED == updateCounts.get(i)) {
                    failedOffsets.add(i);
                }
            }
            if (isHaltedBeforeRemainingMessages) {
                failedOffsets.add(updateCounts.size());
                updateCounts.add(Statement.EXECUTE_FAILED);
            }
            if (failedOffsets.isEmpty()) {
                failedOffsets.add(Math.max(executedCount, updateCounts.size() - 1));
            }
            Iterator<SQLException> causes = getFailureCauses(ex, failedOffsets.size()).iterator();
            for (int each : failedOffsets) {
                failures.putIfAbsent(each, causes.next());
            }
        }
        
        private Collection<SQLException> getFailureCauses(final SQLException ex, final int failedCount) {
            Collection<SQLException> chainedCauses = new LinkedList<>();
            for (SQLException each = ex.getNextException(); null != each; each = each.getNextException()) {
                chainedCauses.add(each);
            }
            return chainedCauses.size() == failedCount ? chainedCauses : Collections.nCopies(failedCount, ex);
        }
        
        @Override
        protected Optional<BatchExecutionUnitResult> getSaneResult(final SQLStatement sqlStatement, final SQLException ex) {
            return Optional.empty();
        }
    }
    
    @RequiredArgsConstructor
    static final class RawTextBlob {
        
        private final byte[] content;
        
        private final int charsetId;
        
        @Nullable
        private final FirebirdReturnColumnPacket parameter;
        
        RawTextBlob(final byte[] content, final int charsetId) {
            this(content, charsetId, null);
        }
    }
    
    @RequiredArgsConstructor
    private static final class BatchExecution {
        
        private final JDBCExecutionUnit executionUnit;
        
        private final int fromOffset;
        
        private final int toOffset;
    }
    
    @RequiredArgsConstructor
    private static final class BatchExecutionUnitResult {
        
        private final int[] updateCounts;
        
        private final Map<Integer, SQLException> failures;
    }
}
