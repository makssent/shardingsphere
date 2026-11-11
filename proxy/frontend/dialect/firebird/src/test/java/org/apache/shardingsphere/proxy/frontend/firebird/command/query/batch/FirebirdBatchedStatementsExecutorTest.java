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

import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.database.exception.core.exception.SQLDialectException;
import org.apache.shardingsphere.database.exception.firebird.exception.FirebirdException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.ConversionErrorException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.DateTimeOutOfRangeException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.NumericOutOfRangeException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.ParameterConversionException;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.prepare.FirebirdReturnColumnPacket;
import org.apache.shardingsphere.infra.binder.context.statement.type.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.connection.kernel.KernelProcessor;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionContext;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.context.SQLUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.ConnectionMode;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.jdbc.StatementOption;
import org.apache.shardingsphere.infra.hint.HintValueContext;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.resource.unit.StorageUnit;
import org.apache.shardingsphere.infra.metadata.database.rule.RuleMetaData;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereColumn;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereTable;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.apache.shardingsphere.infra.session.connection.ConnectionContext;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.parser.rule.SQLParserRule;
import org.apache.shardingsphere.parser.rule.builder.DefaultSQLParserRuleConfigurationBuilder;
import org.apache.shardingsphere.proxy.backend.connector.ProxyDatabaseConnectionManager;
import org.apache.shardingsphere.proxy.backend.connector.jdbc.statement.JDBCBackendStatement;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.FirebirdServerPreparedStatement;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.assignment.InsertValuesSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.InsertColumnsSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.TableNameSegment;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.InsertStatement;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;
import org.apache.shardingsphere.sqltranslator.rule.SQLTranslatorRule;
import org.apache.shardingsphere.sqltranslator.rule.builder.DefaultSQLTranslatorRuleConfigurationBuilder;
import org.apache.shardingsphere.test.infra.framework.extension.mock.AutoMockExtension;
import org.apache.shardingsphere.test.infra.framework.extension.mock.StaticMockSettings;
import org.firebirdsql.extern.decimal.Decimal128;
import org.firebirdsql.extern.decimal.Decimal64;
import org.firebirdsql.gds.GDSExceptionHelper;
import org.firebirdsql.gds.ISCConstants;
import org.firebirdsql.jdbc.JaybirdTypeCodes;
import org.firebirdsql.jdbc.field.TypeConversionException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(AutoMockExtension.class)
@StaticMockSettings(ProxyContext.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class FirebirdBatchedStatementsExecutorTest {
    
    private final DatabaseType databaseType = TypedSPILoader.getService(DatabaseType.class, "Firebird");
    
    @Mock
    private ProxyDatabaseConnectionManager databaseConnectionManager;
    
    @Mock
    private JDBCBackendStatement backendStatement;
    
    @Test
    void assertExecuteBatch() throws SQLException {
        Connection connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        when(connection.getMetaData().getURL()).thenReturn("jdbc:firebirdsql://127.0.0.1/db");
        when(databaseConnectionManager.getConnections(any(), nullable(String.class), anyInt(), anyInt(), any(ConnectionMode.class))).thenReturn(Collections.singletonList(connection));
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(preparedStatement.getConnection()).thenReturn(connection);
        when(preparedStatement.executeBatch()).thenReturn(new int[]{1}, new int[]{2});
        when(backendStatement.createStorageResource(any(ExecutionUnit.class), eq(connection), anyInt(), any(ConnectionMode.class), any(StatementOption.class), nullable(DatabaseType.class)))
                .thenReturn(preparedStatement);
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdPreparedStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)",
                mockInsertStatementContext(), new HintValueContext());
        List<List<Object>> parameterSets = Arrays.asList(Arrays.asList(1, "foo_1"), Arrays.asList(2, "foo_2"));
        FirebirdBatchCompletion actual = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdPreparedStatement, parameterSets, false).executeBatch();
        assertThat(actual.getRecordsCount(), is(2));
        assertThat(actual.getUpdateCounts(), is(new int[]{1, 2}));
        assertTrue(actual.getFailures().isEmpty());
        InOrder inOrder = inOrder(preparedStatement);
        for (List<Object> each : parameterSets) {
            inOrder.verify(preparedStatement).setObject(1, each.get(0));
            inOrder.verify(preparedStatement).setObject(2, each.get(1));
            inOrder.verify(preparedStatement).addBatch();
        }
        inOrder.verify(preparedStatement).executeBatch();
        inOrder.verify(preparedStatement).close();
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("backendConversionFailureArguments")
    void assertMultiErrorExecutesPreviousMessageBeforeBackendConversionFailure(final String name, final String first, final String failure, final String third, final int jdbcType,
                                                                               @Nullable final Throwable cause, final Class<? extends SQLDialectException> expectedErrorType) throws SQLException {
        PreparedStatement statement = mockSameRoutePreparedStatement();
        when(statement.executeBatch()).thenReturn(new int[]{1});
        ParameterMetaData parameterMetaData = mock(ParameterMetaData.class);
        when(statement.getParameterMetaData()).thenReturn(parameterMetaData);
        when(parameterMetaData.getParameterType(2)).thenReturn(jdbcType);
        doThrow(new TypeConversionException("invalid value", cause)).when(statement).setObject(2, failure);
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        List<List<Object>> params = Arrays.asList(Arrays.asList(1, first), Arrays.asList(2, failure), Arrays.asList(3, third));
        FirebirdBatchedStatementsExecutor executor = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdStatement, params, true, Collections.emptyMap(), true);
        ParameterConversionException actual = assertThrows(ParameterConversionException.class, executor::executeBatch);
        assertThat(actual.getConversionError(), isA(expectedErrorType));
        if (actual.getConversionError() instanceof ConversionErrorException) {
            assertThat(((ConversionErrorException) actual.getConversionError()).getValue(), is(failure));
        }
        InOrder inOrder = inOrder(statement);
        inOrder.verify(statement).setObject(1, 1);
        inOrder.verify(statement).setObject(2, first);
        inOrder.verify(statement).addBatch();
        inOrder.verify(statement).setObject(1, 2);
        inOrder.verify(statement).setObject(2, failure);
        inOrder.verify(statement).executeBatch();
        verify(statement, never()).setObject(1, 3);
    }
    
    @Test
    void assertMultiErrorBatchesValidBackendConversions() throws SQLException {
        PreparedStatement statement = mockSameRoutePreparedStatement();
        when(statement.getParameterMetaData()).thenReturn(mock(ParameterMetaData.class));
        when(statement.executeBatch()).thenReturn(new int[]{1, 1, 1});
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        List<List<Object>> params = Arrays.asList(Arrays.asList(1, "10"), Arrays.asList(2, "15"), Arrays.asList(3, "20"));
        FirebirdBatchedStatementsExecutor executor = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdStatement, params, true, Collections.emptyMap(), true);
        FirebirdBatchCompletion actual = executor.executeBatch();
        assertThat(actual.getUpdateCounts(), is(new int[]{1, 1, 1}));
        verify(statement, times(3)).addBatch();
        verify(statement).executeBatch();
    }
    
    @Test
    void assertMultiErrorRetriesFailedPrefixBeforeBackendConversionFailure() throws SQLException {
        PreparedStatement statement = mockSameRoutePreparedStatement();
        when(statement.getParameterMetaData()).thenReturn(mock(ParameterMetaData.class));
        when(statement.executeBatch()).thenThrow(createFailure(new int[]{1})).thenReturn(new int[]{1});
        doThrow(new TypeConversionException("invalid value", new NumberFormatException())).when(statement).setObject(2, "bad");
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        List<List<Object>> params = Arrays.asList(Arrays.asList(1, "10"), Arrays.asList(2, "11"), Arrays.asList(3, "12"), Arrays.asList(4, "bad"), Arrays.asList(5, "20"));
        FirebirdBatchedStatementsExecutor executor = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdStatement, params, true, Collections.emptyMap(), true);
        assertThrows(ParameterConversionException.class, executor::executeBatch);
        verify(statement, times(2)).executeBatch();
        verify(statement, times(2)).setObject(1, 3);
        verify(statement, never()).setObject(1, 5);
    }
    
    private static Stream<Arguments> backendConversionFailureArguments() {
        return Stream.of(
                Arguments.of("invalid_numeric", "10", "bad", "20", Types.NUMERIC, new NumberFormatException(), ConversionErrorException.class),
                Arguments.of("numeric_invalid_mantissa", "10", "bad1e9999", "20", Types.NUMERIC, new NumberFormatException(), ConversionErrorException.class),
                Arguments.of("decimal_missing_mantissa", "10", "e9999", "20", Types.DECIMAL, new NumberFormatException(), ConversionErrorException.class),
                Arguments.of("numeric_overflow", "10", "999999999999999999999999", "20", Types.NUMERIC, null, NumericOutOfRangeException.class),
                Arguments.of("decimal_overflow", "10", "999999999999999999999999", "20", Types.DECIMAL, null, NumericOutOfRangeException.class));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("temporalValueArguments")
    void assertExecuteBatchWithTemporalValue(final String name, final int jdbcType, final String value, final Object expected) throws SQLException {
        PreparedStatement statement = mockSameRoutePreparedStatement();
        ParameterMetaData parameterMetaData = mock(ParameterMetaData.class);
        when(statement.getParameterMetaData()).thenReturn(parameterMetaData);
        when(parameterMetaData.getParameterType(2)).thenReturn(jdbcType);
        when(statement.executeBatch()).thenReturn(new int[]{1});
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        List<List<Object>> params = Collections.singletonList(Arrays.asList(1, value));
        FirebirdBatchCompletion actual = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdStatement, params, false, Collections.emptyMap(), true).executeBatch();
        assertThat(actual.getUpdateCounts(), is(new int[]{1}));
        verify(statement).setObject(2, expected);
        verify(statement.getConnection(), never()).prepareStatement(anyString());
    }
    
    private static Stream<Arguments> temporalValueArguments() {
        return Stream.of(
                Arguments.of("date", Types.DATE, "2001-01-02", LocalDate.of(2001, 1, 2)),
                Arguments.of("leap_date", Types.DATE, "2000-2-29", LocalDate.of(2000, 2, 29)),
                Arguments.of("padded_date", Types.DATE, "\t 2001-01-02 \t", LocalDate.of(2001, 1, 2)),
                Arguments.of("time", Types.TIME, "12:13:14", LocalTime.of(12, 13, 14)),
                Arguments.of("fractional_time", Types.TIME, "12:13:14.1234", LocalTime.of(12, 13, 14, 123400000)),
                Arguments.of("short_fractional_time", Types.TIME, "1:2:3.1", LocalTime.of(1, 2, 3, 100000000)),
                Arguments.of("offset_time", Types.TIME_WITH_TIMEZONE, "12:13:14.1234+03:00", OffsetTime.of(12, 13, 14, 123400000, ZoneOffset.ofHours(3))),
                Arguments.of("offset_time_minutes", Types.TIME_WITH_TIMEZONE, "12:13+03:00", OffsetTime.of(12, 13, 0, 0, ZoneOffset.ofHours(3))),
                Arguments.of("padded_offset_time", Types.TIME_WITH_TIMEZONE, " 12:13:14-04:00 ", OffsetTime.of(12, 13, 14, 0, ZoneOffset.ofHours(-4))),
                Arguments.of("non_padded_timestamp", Types.TIMESTAMP, "2001-1-2 3:4:5", LocalDateTime.of(2001, 1, 2, 3, 4, 5)),
                Arguments.of("timestamp", Types.TIMESTAMP, "2001-01-02 12:13:14.1234", LocalDateTime.of(2001, 1, 2, 12, 13, 14, 123400000)));
    }
    
    @ParameterizedTest(name = "{0}: {2}")
    @MethodSource("nativeTemporalArguments")
    void assertExecuteBatchWithNativeTemporalValue(final String typeName, final int jdbcType, final String value, final Object expectedFirst, final Object expectedSecond) throws SQLException {
        PreparedStatement statement = mockSameRoutePreparedStatement();
        ParameterMetaData parameterMetaData = mock(ParameterMetaData.class);
        when(statement.getParameterMetaData()).thenReturn(parameterMetaData);
        when(parameterMetaData.getParameterType(2)).thenReturn(jdbcType);
        when(statement.executeBatch()).thenReturn(new int[]{1, 1});
        PreparedStatement conversion = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        String sql = "SELECT CAST(CAST(? AS BLOB SUB_TYPE TEXT CHARACTER SET NONE) AS " + typeName + ") FROM RDB$DATABASE";
        when(statement.getConnection().prepareStatement(sql)).thenReturn(conversion);
        when(conversion.executeQuery()).thenReturn(resultSet);
        doReturn(expectedFirst, expectedSecond).when(resultSet).getObject(1, expectedFirst.getClass());
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        List<List<Object>> params = Arrays.asList(Arrays.asList(1, value), Arrays.asList(2, value));
        FirebirdBatchCompletion actual = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdStatement, params, false, Collections.emptyMap(), true).executeBatch();
        assertThat(actual.getUpdateCounts(), is(new int[]{1, 1}));
        InOrder inOrder = inOrder(statement);
        inOrder.verify(statement).setObject(2, expectedFirst);
        inOrder.verify(statement).setObject(2, expectedSecond);
        verify(statement.getConnection()).prepareStatement(sql);
        verify(conversion, times(2)).setBytes(1, value.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1));
        verify(conversion, times(2)).executeQuery();
        verify(resultSet, times(2)).close();
        verify(statement).getParameterMetaData();
        verify(conversion).close();
    }
    
    private static Stream<Arguments> nativeTemporalArguments() {
        return Stream.of(
                Arguments.of("DATE", Types.DATE, "today", LocalDate.of(2001, 1, 2), LocalDate.of(2001, 1, 3)),
                Arguments.of("TIME", Types.TIME, "now", LocalTime.of(12, 13, 14), LocalTime.of(12, 13, 15)),
                Arguments.of("TIME WITH TIME ZONE", Types.TIME_WITH_TIMEZONE, "now",
                        ZonedDateTime.of(2001, 1, 2, 12, 13, 14, 0, ZoneId.of("Europe/Paris")), ZonedDateTime.of(2001, 1, 2, 12, 13, 15, 0, ZoneId.of("Europe/Paris"))),
                Arguments.of("TIME WITH TIME ZONE", Types.TIME_WITH_TIMEZONE, "12:13:14.1234 Europe/Paris",
                        ZonedDateTime.of(2001, 1, 2, 12, 13, 14, 123400000, ZoneId.of("Europe/Paris")), ZonedDateTime.of(2001, 1, 2, 12, 13, 14, 123400000, ZoneId.of("Europe/Paris"))),
                Arguments.of("TIMESTAMP WITH TIME ZONE", Types.TIMESTAMP_WITH_TIMEZONE, "now",
                        ZonedDateTime.of(2001, 1, 2, 12, 13, 14, 0, ZoneId.of("Europe/Paris")), ZonedDateTime.of(2001, 1, 2, 12, 13, 15, 0, ZoneId.of("Europe/Paris"))),
                Arguments.of("TIMESTAMP WITH TIME ZONE", Types.TIMESTAMP_WITH_TIMEZONE, "2001-01-02 12:13:14.1234+03:00",
                        ZonedDateTime.of(2001, 1, 2, 12, 13, 14, 123400000, ZoneOffset.ofHours(3)), ZonedDateTime.of(2001, 1, 2, 12, 13, 14, 123400000, ZoneOffset.ofHours(3))),
                Arguments.of("TIMESTAMP WITH TIME ZONE", Types.TIMESTAMP_WITH_TIMEZONE, "2001-01-02 12:13:14 Europe/Paris",
                        ZonedDateTime.of(2001, 1, 2, 12, 13, 14, 0, ZoneId.of("Europe/Paris")), ZonedDateTime.of(2001, 1, 2, 12, 13, 14, 0, ZoneId.of("Europe/Paris"))),
                Arguments.of("TIMESTAMP", Types.TIMESTAMP, "now", LocalDateTime.of(2001, 1, 2, 12, 13, 14), LocalDateTime.of(2001, 1, 2, 12, 13, 15)));
    }
    
    @ParameterizedTest(name = "{0}: {3}")
    @MethodSource("nativeScalarArguments")
    void assertExecuteBatchWithNativeScalarValue(final String typeName, final int jdbcType, final int precision, final String first, final String second, final Object expectedFirst,
                                                 final Object expectedSecond) throws SQLException {
        PreparedStatement statement = mockSameRoutePreparedStatement();
        ParameterMetaData parameterMetaData = mock(ParameterMetaData.class);
        when(statement.getParameterMetaData()).thenReturn(parameterMetaData);
        when(parameterMetaData.getParameterType(2)).thenReturn(jdbcType);
        if (JaybirdTypeCodes.DECFLOAT == jdbcType) {
            when(parameterMetaData.getPrecision(2)).thenReturn(precision);
        }
        if (Types.NUMERIC == jdbcType) {
            when(parameterMetaData.getParameterTypeName(2)).thenReturn(typeName);
        }
        when(statement.executeBatch()).thenReturn(new int[]{1, 1});
        PreparedStatement conversion = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        String sql = "SELECT CAST(CAST(? AS BLOB SUB_TYPE TEXT CHARACTER SET NONE) AS " + typeName + ") FROM RDB$DATABASE";
        when(statement.getConnection().prepareStatement(sql)).thenReturn(conversion);
        when(conversion.executeQuery()).thenReturn(resultSet);
        if (JaybirdTypeCodes.DECFLOAT == jdbcType) {
            doReturn(expectedFirst, expectedSecond).when(resultSet).getObject(1, expectedFirst.getClass());
        } else {
            when(resultSet.getObject(1)).thenReturn(expectedFirst, expectedSecond);
        }
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        List<List<Object>> params = Arrays.asList(Arrays.asList(1, first), Arrays.asList(2, second));
        FirebirdBatchCompletion actual = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdStatement, params, false, Collections.emptyMap(), true).executeBatch();
        assertThat(actual.getUpdateCounts(), is(new int[]{1, 1}));
        InOrder inOrder = inOrder(statement);
        inOrder.verify(statement).setObject(2, expectedFirst);
        inOrder.verify(statement).setObject(2, expectedSecond);
        verify(statement.getConnection()).prepareStatement(sql);
        verify(conversion).setBytes(1, first.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1));
        verify(conversion).setBytes(1, second.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1));
        verify(conversion, times(2)).executeQuery();
        verify(resultSet, times(2)).close();
        verify(statement).getParameterMetaData();
        verify(conversion).close();
    }
    
    private static Stream<Arguments> nativeScalarArguments() {
        return Stream.of(
                Arguments.of("BOOLEAN", Types.BOOLEAN, 0, "true", "false", true, false),
                Arguments.of("FLOAT", Types.FLOAT, 0, "2.5", "4.5", 2.5F, 4.5F),
                Arguments.of("DOUBLE PRECISION", Types.DOUBLE, 0, "1e39", "2.5", 1.0000000000000001E39, 2.5D),
                Arguments.of("INT128", Types.NUMERIC, 0, "0xF", new String(new char[256]).replace('\0', '0') + "1", new BigDecimal("15"), BigDecimal.ONE),
                Arguments.of("DECFLOAT(16)", JaybirdTypeCodes.DECFLOAT, 16, "2.5", "3.5", Decimal64.valueOf("2.5"), Decimal64.valueOf("3.5")),
                Arguments.of("DECFLOAT(34)", JaybirdTypeCodes.DECFLOAT, 34, "2.5", "3.5", Decimal128.valueOf("2.5"), Decimal128.valueOf("3.5")),
                Arguments.of("DECFLOAT(16)", JaybirdTypeCodes.DECFLOAT, 16, "NaN", "Infinity", Decimal64.valueOf("NaN"), Decimal64.valueOf("Infinity")),
                Arguments.of("DECFLOAT(34)", JaybirdTypeCodes.DECFLOAT, 34, "NaN", "-Infinity", Decimal128.valueOf("NaN"), Decimal128.valueOf("-Infinity")),
                Arguments.of("DECFLOAT(16)", JaybirdTypeCodes.DECFLOAT, 16, "-0", "0", Decimal64.valueOf("-0"), Decimal64.valueOf("0")),
                Arguments.of("DECFLOAT(34)", JaybirdTypeCodes.DECFLOAT, 34, "-0", "0", Decimal128.valueOf("-0"), Decimal128.valueOf("0")));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("otherStorageScalarArguments")
    void assertExecuteBatchWithOtherStorageScalar(final int jdbcType, final String value) throws SQLException {
        PreparedStatement statement = mockSameRoutePreparedStatement();
        when(statement.getConnection().getMetaData().getURL()).thenReturn("jdbc:mysql://127.0.0.1/db");
        ParameterMetaData parameterMetaData = mock(ParameterMetaData.class);
        when(statement.getParameterMetaData()).thenReturn(parameterMetaData);
        when(parameterMetaData.getParameterType(2)).thenReturn(jdbcType);
        when(statement.executeBatch()).thenReturn(new int[]{1, 1});
        ContextManager contextManager = mockContextManager();
        StorageUnit storageUnit = contextManager.getMetaDataContexts().getMetaData().getDatabase("db").getResourceMetaData().getStorageUnits().get("ds_0");
        when(storageUnit.getStorageType()).thenReturn(TypedSPILoader.getService(DatabaseType.class, "MySQL"));
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        List<List<Object>> params = Arrays.asList(Arrays.asList(1, value), Arrays.asList(2, value));
        FirebirdBatchCompletion actual = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdStatement, params, false, Collections.emptyMap(), true).executeBatch();
        assertThat(actual.getUpdateCounts(), is(new int[]{1, 1}));
        verify(statement, times(2)).setObject(2, value);
        verify(statement.getConnection(), never()).prepareStatement(anyString());
        verify(statement.getConnection().getMetaData()).getURL();
    }
    
    private static Stream<Arguments> otherStorageScalarArguments() {
        return Stream.of(Arguments.of(Types.FLOAT, "2.5"), Arguments.of(Types.DOUBLE, "1.25"), Arguments.of(Types.BOOLEAN, "true"));
    }
    
    @ParameterizedTest(name = "{0}: {3}")
    @MethodSource("nativeScalarFailureArguments")
    void assertExecuteBatchWithNativeScalarFailure(final String typeName, final int jdbcType, final int precision, final String failure, final int errorCode,
                                                   final Class<? extends Exception> expectedErrorType) throws SQLException {
        PreparedStatement statement = mockSameRoutePreparedStatement();
        ParameterMetaData parameterMetaData = mock(ParameterMetaData.class);
        when(statement.getParameterMetaData()).thenReturn(parameterMetaData);
        when(parameterMetaData.getParameterType(2)).thenReturn(jdbcType);
        if (JaybirdTypeCodes.DECFLOAT == jdbcType) {
            when(parameterMetaData.getPrecision(2)).thenReturn(precision);
        }
        if (Types.NUMERIC == jdbcType) {
            when(parameterMetaData.getParameterTypeName(2)).thenReturn(typeName);
        }
        when(statement.executeBatch()).thenReturn(new int[]{1});
        PreparedStatement conversion = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        String sql = "SELECT CAST(CAST(? AS BLOB SUB_TYPE TEXT CHARACTER SET NONE) AS " + typeName + ") FROM RDB$DATABASE";
        when(statement.getConnection().prepareStatement(sql)).thenReturn(conversion);
        when(conversion.executeQuery()).thenReturn(resultSet).thenThrow(new SQLException(GDSExceptionHelper.getMessage(errorCode).toString(), "22018", errorCode));
        if (JaybirdTypeCodes.DECFLOAT == jdbcType) {
            if (16 == precision) {
                when(resultSet.getObject(1, Decimal64.class)).thenReturn(Decimal64.valueOf("1"));
            } else {
                when(resultSet.getObject(1, Decimal128.class)).thenReturn(Decimal128.valueOf("1"));
            }
        } else {
            when(resultSet.getObject(1)).thenReturn(Types.BOOLEAN == jdbcType ? true : BigDecimal.ONE);
        }
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        String first = Types.BOOLEAN == jdbcType ? "true" : "1";
        List<List<Object>> params = Arrays.asList(Arrays.asList(1, first), Arrays.asList(2, failure), Arrays.asList(3, first));
        FirebirdBatchedStatementsExecutor executor = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdStatement, params, true, Collections.emptyMap(), true);
        ParameterConversionException actual = assertThrows(ParameterConversionException.class, executor::executeBatch);
        assertThat(actual.getConversionError(), isA(expectedErrorType));
        if (actual.getConversionError() instanceof SQLException) {
            assertThat(((SQLException) actual.getConversionError()).getErrorCode(), is(errorCode));
        }
        verify(statement).executeBatch();
        verify(statement, never()).setObject(1, 3);
        verify(resultSet).close();
        verify(conversion).close();
    }
    
    private static Stream<Arguments> nativeScalarFailureArguments() {
        return Stream.of(
                Arguments.of("BOOLEAN", Types.BOOLEAN, 0, "0", ISCConstants.isc_convert_error, ConversionErrorException.class),
                Arguments.of("FLOAT", Types.FLOAT, 0, "1e39", ISCConstants.isc_numeric_out_of_range, NumericOutOfRangeException.class),
                Arguments.of("FLOAT", Types.FLOAT, 0, "ÿ", ISCConstants.isc_convert_error, ConversionErrorException.class),
                Arguments.of("DOUBLE PRECISION", Types.DOUBLE, 0, "1e309", ISCConstants.isc_numeric_out_of_range, NumericOutOfRangeException.class),
                Arguments.of("INT128", Types.NUMERIC, 0, "1e39", ISCConstants.isc_numeric_out_of_range, NumericOutOfRangeException.class),
                Arguments.of("DECFLOAT(16)", JaybirdTypeCodes.DECFLOAT, 16, "bad", ISCConstants.isc_decfloat_invalid_operation, FirebirdException.class),
                Arguments.of("DECFLOAT(34)", JaybirdTypeCodes.DECFLOAT, 34, "1e9999", ISCConstants.isc_decfloat_overflow, FirebirdException.class));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidTemporalArguments")
    void assertExecuteBatchWithInvalidTemporalValue(final String name, final String typeName, final int jdbcType, final String first, final String failure,
                                                    final Object expectedFirst) throws SQLException {
        PreparedStatement statement = mockSameRoutePreparedStatement();
        ParameterMetaData parameterMetaData = mock(ParameterMetaData.class);
        when(statement.getParameterMetaData()).thenReturn(parameterMetaData);
        when(parameterMetaData.getParameterType(2)).thenReturn(jdbcType);
        when(statement.executeBatch()).thenReturn(new int[]{1});
        PreparedStatement conversion = mock(PreparedStatement.class);
        String sql = "SELECT CAST(CAST(? AS BLOB SUB_TYPE TEXT CHARACTER SET NONE) AS " + typeName + ") FROM RDB$DATABASE";
        when(statement.getConnection().prepareStatement(sql)).thenReturn(conversion);
        when(conversion.executeQuery()).thenThrow(new SQLException("conversion error", "22018", 335544334));
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        List<List<Object>> params = Arrays.asList(Arrays.asList(1, first), Arrays.asList(2, failure), Arrays.asList(3, first));
        FirebirdBatchedStatementsExecutor executor = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdStatement, params, true, Collections.emptyMap(), true);
        ParameterConversionException actual = assertThrows(ParameterConversionException.class, executor::executeBatch);
        assertThat(actual.getConversionError(), isA(ConversionErrorException.class));
        assertThat(((ConversionErrorException) actual.getConversionError()).getValue(), is(failure));
        InOrder inOrder = inOrder(statement);
        inOrder.verify(statement).setObject(2, expectedFirst);
        inOrder.verify(statement).addBatch();
        inOrder.verify(statement).executeBatch();
        verify(statement, never()).setObject(1, 3);
        verify(conversion).close();
    }
    
    private static Stream<Arguments> invalidTemporalArguments() {
        return Stream.of(
                Arguments.of("invalid_date", "DATE", Types.DATE, "2001-01-02", "bad", LocalDate.of(2001, 1, 2)),
                Arguments.of("invalid_calendar_date", "DATE", Types.DATE, "2001-01-02", "2026-02-31", LocalDate.of(2001, 1, 2)),
                Arguments.of("invalid_time", "TIME", Types.TIME, "12:13:14", "25:00:00", LocalTime.of(12, 13, 14)),
                Arguments.of("invalid_offset_time", "TIME WITH TIME ZONE", Types.TIME_WITH_TIMEZONE, "12:13:14+03:00", "25:00:00+03:00", OffsetTime.of(12, 13, 14, 0, ZoneOffset.ofHours(3))),
                Arguments.of("invalid_timestamp", "TIMESTAMP", Types.TIMESTAMP, "2001-01-02 12:13:14", "bad", LocalDateTime.of(2001, 1, 2, 12, 13, 14)));
    }
    
    @Test
    void assertExecuteBatchWithInvalidNativeZonedTimestamp() throws SQLException {
        PreparedStatement statement = mockSameRoutePreparedStatement();
        ParameterMetaData parameterMetaData = mock(ParameterMetaData.class);
        when(statement.getParameterMetaData()).thenReturn(parameterMetaData);
        when(parameterMetaData.getParameterType(2)).thenReturn(Types.TIMESTAMP_WITH_TIMEZONE);
        when(statement.executeBatch()).thenReturn(new int[]{1});
        PreparedStatement conversion = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        String sql = "SELECT CAST(CAST(? AS BLOB SUB_TYPE TEXT CHARACTER SET NONE) AS TIMESTAMP WITH TIME ZONE) FROM RDB$DATABASE";
        when(statement.getConnection().prepareStatement(sql)).thenReturn(conversion);
        when(conversion.executeQuery()).thenReturn(resultSet).thenThrow(new SQLException("conversion error", "22018", ISCConstants.isc_convert_error));
        ZonedDateTime expected = ZonedDateTime.of(2001, 1, 2, 12, 13, 14, 0, ZoneId.of("Europe/Paris"));
        when(resultSet.getObject(1, ZonedDateTime.class)).thenReturn(expected);
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        List<List<Object>> params = Arrays.asList(Arrays.asList(1, "now"), Arrays.asList(2, "bad"), Arrays.asList(3, "now"));
        FirebirdBatchedStatementsExecutor executor = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdStatement, params, true, Collections.emptyMap(), true);
        ParameterConversionException actual = assertThrows(ParameterConversionException.class, executor::executeBatch);
        assertThat(actual.getConversionError(), isA(ConversionErrorException.class));
        assertThat(((ConversionErrorException) actual.getConversionError()).getValue(), is("bad"));
        InOrder inOrder = inOrder(statement);
        inOrder.verify(statement).setObject(2, expected);
        inOrder.verify(statement).addBatch();
        inOrder.verify(statement).executeBatch();
        verify(statement, never()).setObject(1, 3);
        verify(resultSet).close();
        verify(conversion).close();
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("temporalRangeArguments")
    void assertExecuteBatchWithTemporalRangeFailure(final String typeName, final int jdbcType, final int errorCode, final String first) throws SQLException {
        PreparedStatement statement = mockSameRoutePreparedStatement();
        ParameterMetaData parameterMetaData = mock(ParameterMetaData.class);
        when(statement.getParameterMetaData()).thenReturn(parameterMetaData);
        when(parameterMetaData.getParameterType(2)).thenReturn(jdbcType);
        when(statement.executeBatch()).thenReturn(new int[]{1});
        PreparedStatement conversion = mock(PreparedStatement.class);
        String sql = "SELECT CAST(CAST(? AS BLOB SUB_TYPE TEXT CHARACTER SET NONE) AS " + typeName + ") FROM RDB$DATABASE";
        when(statement.getConnection().prepareStatement(sql)).thenReturn(conversion);
        when(conversion.executeQuery()).thenThrow(new SQLException("native range failure", "22008", errorCode));
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        List<List<Object>> params = Arrays.asList(Arrays.asList(1, first), Arrays.asList(2, "foo_native_range"), Arrays.asList(3, first));
        FirebirdBatchedStatementsExecutor executor = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdStatement, params, true, Collections.emptyMap(), true);
        ParameterConversionException actual = assertThrows(ParameterConversionException.class, executor::executeBatch);
        assertThat(actual.getConversionError(), isA(DateTimeOutOfRangeException.class));
        assertThat(((DateTimeOutOfRangeException) actual.getConversionError()).getJdbcType(), is(jdbcType));
        verify(statement).executeBatch();
        verify(statement, never()).setObject(1, 3);
        verify(conversion).close();
    }
    
    private static Stream<Arguments> temporalRangeArguments() {
        return Stream.of(
                Arguments.of("DATE", Types.DATE, ISCConstants.isc_date_range_exceeded, "2001-01-02"),
                Arguments.of("TIME", Types.TIME, ISCConstants.isc_time_range_exceeded, "12:13:14"),
                Arguments.of("TIME WITH TIME ZONE", Types.TIME_WITH_TIMEZONE, ISCConstants.isc_time_range_exceeded, "12:13:14+03:00"),
                Arguments.of("TIMESTAMP", Types.TIMESTAMP, ISCConstants.isc_datetime_range_exceeded, "2001-01-02 12:13:14"));
    }
    
    @Test
    void assertExecuteBatchWithTemporalConnectionFailure() throws SQLException {
        PreparedStatement statement = mockSameRoutePreparedStatement();
        ParameterMetaData parameterMetaData = mock(ParameterMetaData.class);
        when(statement.getParameterMetaData()).thenReturn(parameterMetaData);
        when(parameterMetaData.getParameterType(2)).thenReturn(Types.DATE);
        SQLException failure = new SQLException("connection lost", "08006", ISCConstants.isc_network_error);
        when(statement.getConnection().prepareStatement(anyString())).thenThrow(failure);
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        List<List<Object>> params = Collections.singletonList(Arrays.asList(1, "today"));
        FirebirdBatchedStatementsExecutor executor = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdStatement, params, true, Collections.emptyMap(), true);
        SQLException actual = assertThrows(SQLException.class, executor::executeBatch);
        assertThat(actual, is(failure));
        verify(statement, never()).executeBatch();
        verify(statement).close();
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("textColumnCharsetArguments")
    void assertExecuteBatchWithTextColumnCharset(final String name, final int charsetId, final Object expected) throws SQLException {
        Connection connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        when(connection.getMetaData().getURL()).thenReturn("jdbc:firebirdsql://127.0.0.1/db");
        when(databaseConnectionManager.getConnections(any(), nullable(String.class), anyInt(), anyInt(), any(ConnectionMode.class))).thenReturn(Collections.singletonList(connection));
        PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.getConnection()).thenReturn(connection);
        when(statement.executeBatch()).thenReturn(new int[]{1});
        when(backendStatement.createStorageResource(any(ExecutionUnit.class), eq(connection), anyInt(), any(ConnectionMode.class), any(StatementOption.class), nullable(DatabaseType.class)))
                .thenReturn(statement);
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        ShardingSphereTable table = new ShardingSphereTable("t", Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        ShardingSphereColumn column = new ShardingSphereColumn("col", Types.VARCHAR, false, false, false, true, false, false);
        FirebirdReturnColumnPacket parameter = new FirebirdReturnColumnPacket(Collections.emptyList(), 2, table, column, null, null, null, 10, false, null);
        parameter.setTargetCharacterSetId(charsetId);
        String value = new String("яa");
        byte[] originalBytes = {(byte) 0xFF, 'a'};
        Map<String, FirebirdBatchedStatementsExecutor.RawTextBlob> rawTextBlobs = new IdentityHashMap<>();
        rawTextBlobs.put(value, new FirebirdBatchedStatementsExecutor.RawTextBlob(originalBytes, 52, parameter));
        new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdStatement, Collections.singletonList(Arrays.asList(1, value)), false, rawTextBlobs).executeBatch();
        ArgumentCaptor<Object> actualValue = ArgumentCaptor.forClass(Object.class);
        verify(statement).setObject(eq(2), actualValue.capture());
        assertThat(actualValue.getValue(), is(expected));
        ArgumentCaptor<ExecutionUnit> actualUnit = ArgumentCaptor.forClass(ExecutionUnit.class);
        verify(backendStatement).createStorageResource(actualUnit.capture(), eq(connection), anyInt(), any(ConnectionMode.class), any(StatementOption.class), nullable(DatabaseType.class));
        assertThat(actualUnit.getValue().getSqlUnit().getSql(), is("INSERT INTO t (id, col) VALUES (?, ?)"));
    }
    
    private static Stream<Arguments> textColumnCharsetArguments() {
        return Stream.of(Arguments.of("none", 0, new byte[]{(byte) 0xFF, 'a'}), Arguments.of("utf8", 4, "яa"), Arguments.of("win1251", 52, "яa"));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("textBlobCharsetArguments")
    void assertExecuteBatchWithTextBlobCharset(final String name, final int sourceCharset, final String charsetName, final Object value) throws SQLException {
        Connection connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        when(connection.getMetaData().getURL()).thenReturn("jdbc:firebirdsql://127.0.0.1/db");
        when(databaseConnectionManager.getConnections(any(), nullable(String.class), anyInt(), anyInt(), any(ConnectionMode.class))).thenReturn(Collections.singletonList(connection));
        PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.getConnection()).thenReturn(connection);
        when(statement.executeBatch()).thenReturn(new int[]{1});
        when(backendStatement.createStorageResource(any(ExecutionUnit.class), eq(connection), anyInt(), any(ConnectionMode.class), any(StatementOption.class), nullable(DatabaseType.class)))
                .thenReturn(statement);
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        byte[] originalBytes = value instanceof byte[] ? (byte[]) value : new byte[]{(byte) 0xFF};
        Map<Object, FirebirdBatchedStatementsExecutor.RawTextBlob> rawTextBlobs = new IdentityHashMap<>();
        rawTextBlobs.put(value, new FirebirdBatchedStatementsExecutor.RawTextBlob(originalBytes, sourceCharset));
        new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdStatement, Collections.singletonList(Arrays.asList(1, value)), false, rawTextBlobs).executeBatch();
        verify(statement).setObject(2, originalBytes);
        ArgumentCaptor<ExecutionUnit> executionUnit = ArgumentCaptor.forClass(ExecutionUnit.class);
        verify(backendStatement).createStorageResource(executionUnit.capture(), eq(connection), anyInt(), any(ConnectionMode.class), any(StatementOption.class), nullable(DatabaseType.class));
        assertThat(executionUnit.getValue().getSqlUnit().getSql(), containsString("CAST(CAST(? AS BLOB SUB_TYPE TEXT CHARACTER SET NONE) AS BLOB SUB_TYPE TEXT CHARACTER SET " + charsetName + ")"));
    }
    
    private static Stream<Arguments> textBlobCharsetArguments() {
        return Stream.of(
                Arguments.of("utf8", 4, "UTF8", "\0"),
                Arguments.of("ascii", 2, "ASCII", "\0"),
                Arguments.of("win1251", 52, "WIN1251", "я"),
                Arguments.of("raw_utf8", 4, "UTF8", new byte[]{(byte) 0xD1, (byte) 0x8F}),
                Arguments.of("raw_win1251", 52, "WIN1251", new byte[]{(byte) 0xFF}),
                Arguments.of("raw_next_without_java_charset", 19, "NEXT", new byte[]{(byte) 0xFF}),
                Arguments.of("none", 0, "NONE", "\0"),
                Arguments.of("octets", 1, "OCTETS", "\0"));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("textBlobSqlMarkerArguments")
    void assertTextBlobRewriteUsesPhysicalParameterMarkers(final String name, final String sql, final String expectedSql) throws SQLException {
        Connection connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        when(connection.getMetaData().getURL()).thenReturn("jdbc:firebirdsql://127.0.0.1/db");
        when(databaseConnectionManager.getConnections(any(), nullable(String.class), anyInt(), anyInt(), any(ConnectionMode.class))).thenReturn(Collections.singletonList(connection));
        PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.getConnection()).thenReturn(connection);
        when(statement.executeBatch()).thenReturn(new int[]{1});
        when(backendStatement.createStorageResource(any(ExecutionUnit.class), eq(connection), anyInt(), any(ConnectionMode.class), any(StatementOption.class), nullable(DatabaseType.class)))
                .thenReturn(statement);
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        String value = new String("\0");
        byte[] originalBytes = {(byte) 0xFF};
        List<Object> params = Arrays.asList(1, value);
        Map<String, FirebirdBatchedStatementsExecutor.RawTextBlob> rawTextBlobs = new IdentityHashMap<>();
        rawTextBlobs.put(value, new FirebirdBatchedStatementsExecutor.RawTextBlob(originalBytes, 4));
        ExecutionUnit unit = new ExecutionUnit("ds_0", new SQLUnit(sql, params));
        try (
                MockedConstruction<KernelProcessor> ignored = mockConstruction(KernelProcessor.class,
                        (mock, context) -> when(mock.generateExecutionContext(any(), any(), any()))
                                .thenReturn(new ExecutionContext(null, Collections.singletonList(unit), mock(RouteContext.class))))) {
            new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdStatement, Collections.singletonList(params), false, rawTextBlobs).executeBatch();
        }
        ArgumentCaptor<ExecutionUnit> actual = ArgumentCaptor.forClass(ExecutionUnit.class);
        verify(backendStatement).createStorageResource(actual.capture(), eq(connection), anyInt(), any(ConnectionMode.class), any(StatementOption.class), nullable(DatabaseType.class));
        assertThat(actual.getValue().getSqlUnit().getSql(), is(expectedSql));
        verify(statement).setObject(2, originalBytes);
    }
    
    private static Stream<Arguments> textBlobSqlMarkerArguments() {
        String cast = "CAST(CAST(? AS BLOB SUB_TYPE TEXT CHARACTER SET NONE) AS BLOB SUB_TYPE TEXT CHARACTER SET UTF8)";
        return Stream.of(
                Arguments.of("quoted_and_commented", "INSERT INTO t (id, col) VALUES (?, COALESCE('😀?', ?)) /* ? */",
                        "INSERT INTO t (id, col) VALUES (?, COALESCE('😀?', " + cast + ")) /* ? */"),
                Arguments.of("gen_id", "INSERT INTO t (id, col) VALUES (GEN_ID(GEN_TEST, ?), ?)", "INSERT INTO t (id, col) VALUES (GEN_ID(GEN_TEST, ?), " + cast + ")"),
                Arguments.of("merge", "MERGE INTO t x USING (SELECT ? AS id FROM RDB$DATABASE) s ON (x.id = s.id) WHEN MATCHED THEN UPDATE SET col = ?",
                        "MERGE INTO t x USING (SELECT ? AS id FROM RDB$DATABASE) s ON (x.id = s.id) WHEN MATCHED THEN UPDATE SET col = " + cast));
    }
    
    @Test
    void assertTextBlobBytesArePreservedWhenBatchRetries() throws SQLException {
        PreparedStatement statement = mockSameRoutePreparedStatement();
        when(statement.executeBatch()).thenThrow(createFailure(new int[]{1})).thenReturn(new int[]{1});
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        String first = new String("\0");
        String second = new String("\0");
        String third = new String("\0");
        byte[] firstBytes = {(byte) 0xFF};
        byte[] secondBytes = {(byte) 0xFE};
        byte[] thirdBytes = {(byte) 0xFD};
        Map<String, FirebirdBatchedStatementsExecutor.RawTextBlob> rawTextBlobs = new IdentityHashMap<>();
        rawTextBlobs.put(first, new FirebirdBatchedStatementsExecutor.RawTextBlob(firstBytes, 4));
        rawTextBlobs.put(second, new FirebirdBatchedStatementsExecutor.RawTextBlob(secondBytes, 4));
        rawTextBlobs.put(third, new FirebirdBatchedStatementsExecutor.RawTextBlob(thirdBytes, 4));
        List<List<Object>> params = Arrays.asList(Arrays.asList(1, first), Arrays.asList(2, second), Arrays.asList(3, third));
        FirebirdBatchCompletion actual = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdStatement, params, true, rawTextBlobs).executeBatch();
        assertThat(actual.getUpdateCounts(), is(new int[]{1, FirebirdBatchCompletion.EXECUTE_FAILED, 1}));
        verify(statement).setObject(2, firstBytes);
        verify(statement).setObject(2, secondBytes);
        verify(statement, times(2)).setObject(2, thirdBytes);
    }
    
    @Test
    void assertMultiErrorKeepsMessageOrderAcrossTextBlobSqlVariants() throws SQLException {
        Connection connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        when(connection.getMetaData().getURL()).thenReturn("jdbc:firebirdsql://127.0.0.1/db");
        when(databaseConnectionManager.getConnections(any(), nullable(String.class), anyInt(), anyInt(), any(ConnectionMode.class))).thenReturn(Collections.singletonList(connection));
        PreparedStatement plainStatement = mock(PreparedStatement.class);
        PreparedStatement blobStatement = mock(PreparedStatement.class);
        when(plainStatement.getConnection()).thenReturn(connection);
        when(blobStatement.getConnection()).thenReturn(connection);
        when(plainStatement.executeBatch()).thenReturn(new int[]{1}, new int[]{1});
        when(blobStatement.executeBatch()).thenThrow(createFailure(new int[0]));
        when(backendStatement.createStorageResource(any(ExecutionUnit.class), eq(connection), anyInt(), any(ConnectionMode.class), any(StatementOption.class), nullable(DatabaseType.class)))
                .thenAnswer(invocation -> invocation.<ExecutionUnit>getArgument(0).getSqlUnit().getSql().contains("CAST(") ? blobStatement : plainStatement);
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        String value = new String("\0");
        Map<String, FirebirdBatchedStatementsExecutor.RawTextBlob> rawTextBlobs = new IdentityHashMap<>();
        rawTextBlobs.put(value, new FirebirdBatchedStatementsExecutor.RawTextBlob(new byte[]{(byte) 0xFF}, 4));
        List<List<Object>> params = Arrays.asList(Arrays.asList(1, null), Arrays.asList(2, value), Arrays.asList(3, null));
        FirebirdBatchCompletion actual = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdStatement, params, true, rawTextBlobs).executeBatch();
        assertThat(actual.getUpdateCounts(), is(new int[]{1, FirebirdBatchCompletion.EXECUTE_FAILED, 1}));
        InOrder inOrder = inOrder(plainStatement, blobStatement);
        inOrder.verify(plainStatement).setObject(1, 1);
        inOrder.verify(plainStatement).executeBatch();
        inOrder.verify(blobStatement).setObject(1, 2);
        inOrder.verify(blobStatement).executeBatch();
        inOrder.verify(plainStatement).setObject(1, 3);
        inOrder.verify(plainStatement).executeBatch();
    }
    
    @Test
    void assertExecuteBatchWithMultiRouteMessageCounts() throws SQLException {
        Connection connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        when(connection.getMetaData().getURL()).thenReturn("jdbc:firebirdsql://127.0.0.1/db");
        when(databaseConnectionManager.getConnections(any(), nullable(String.class), anyInt(), anyInt(), any(ConnectionMode.class))).thenReturn(Collections.singletonList(connection));
        PreparedStatement firstPreparedStatement = mock(PreparedStatement.class);
        PreparedStatement secondPreparedStatement = mock(PreparedStatement.class);
        when(firstPreparedStatement.getConnection()).thenReturn(connection);
        when(secondPreparedStatement.getConnection()).thenReturn(connection);
        when(firstPreparedStatement.executeBatch()).thenReturn(new int[]{1});
        when(secondPreparedStatement.executeBatch()).thenReturn(new int[]{1});
        when(backendStatement.createStorageResource(any(ExecutionUnit.class), eq(connection), anyInt(), any(ConnectionMode.class), any(StatementOption.class), nullable(DatabaseType.class)))
                .thenReturn(firstPreparedStatement, secondPreparedStatement);
        ContextManager contextManager = mockContextManager("ds_0", "ds_1");
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdPreparedStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)", mockInsertStatementContext(), new HintValueContext());
        List<Object> params = Arrays.asList(1, "foo_1");
        ExecutionUnit firstExecutionUnit = new ExecutionUnit("ds_0", new SQLUnit(firebirdPreparedStatement.getSql(), params));
        ExecutionUnit secondExecutionUnit = new ExecutionUnit("ds_1", new SQLUnit(firebirdPreparedStatement.getSql(), params));
        try (
                MockedConstruction<KernelProcessor> ignored = mockConstruction(KernelProcessor.class,
                        (mock, context) -> when(mock.generateExecutionContext(any(), any(), any()))
                                .thenReturn(new ExecutionContext(null, Arrays.asList(firstExecutionUnit, secondExecutionUnit), mock(RouteContext.class))))) {
            FirebirdBatchCompletion actual = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdPreparedStatement, Collections.singletonList(params), false).executeBatch();
            assertThat(actual.getRecordsCount(), is(1));
            assertThat(actual.getUpdateCounts(), is(new int[]{2}));
            assertTrue(actual.getFailures().isEmpty());
        }
    }
    
    @Test
    void assertExecuteBatchWithMultiRouteFailureTranslatesToClientIndexes() throws SQLException {
        Connection connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        when(connection.getMetaData().getURL()).thenReturn("jdbc:firebirdsql://127.0.0.1/db");
        when(databaseConnectionManager.getConnections(any(), nullable(String.class), anyInt(), anyInt(), any(ConnectionMode.class))).thenReturn(Collections.singletonList(connection));
        PreparedStatement firstPreparedStatement = mock(PreparedStatement.class);
        PreparedStatement secondPreparedStatement = mock(PreparedStatement.class);
        when(firstPreparedStatement.getConnection()).thenReturn(connection);
        when(secondPreparedStatement.getConnection()).thenReturn(connection);
        when(firstPreparedStatement.executeBatch()).thenReturn(new int[]{1});
        BatchUpdateException failure = new BatchUpdateException("violation of PRIMARY or UNIQUE KEY constraint", "23000", 335544665, new int[0]);
        when(secondPreparedStatement.executeBatch()).thenThrow(failure);
        when(backendStatement.createStorageResource(any(ExecutionUnit.class), eq(connection), anyInt(), any(ConnectionMode.class), any(StatementOption.class), nullable(DatabaseType.class)))
                .thenReturn(firstPreparedStatement, secondPreparedStatement);
        ContextManager contextManager = mockContextManager("ds_0", "ds_1");
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdPreparedStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)",
                mockInsertStatementContext(), new HintValueContext());
        List<Object> firstParams = Arrays.asList(1, "foo_1");
        List<Object> secondParams = Arrays.asList(2, "foo_2");
        ExecutionUnit firstExecutionUnit = new ExecutionUnit("ds_0", new SQLUnit(firebirdPreparedStatement.getSql(), firstParams));
        ExecutionUnit secondExecutionUnit = new ExecutionUnit("ds_1", new SQLUnit(firebirdPreparedStatement.getSql(), secondParams));
        try (
                MockedConstruction<KernelProcessor> ignored = mockConstruction(KernelProcessor.class,
                        (mock, context) -> when(mock.generateExecutionContext(any(), any(), any()))
                                .thenReturn(new ExecutionContext(null, Collections.singletonList(firstExecutionUnit), mock(RouteContext.class)),
                                        new ExecutionContext(null, Collections.singletonList(secondExecutionUnit), mock(RouteContext.class))))) {
            FirebirdBatchCompletion actual = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdPreparedStatement,
                    Arrays.asList(firstParams, secondParams), false).executeBatch();
            assertThat(actual.getRecordsCount(), is(2));
            assertThat(actual.getUpdateCounts(), is(new int[]{1, FirebirdBatchCompletion.EXECUTE_FAILED}));
            assertThat(actual.getFailures().size(), is(1));
            FirebirdBatchCompletion.Failure actualFailure = actual.getFailures().iterator().next();
            assertThat(actualFailure.getMessageIndex(), is(1));
            assertThat(actualFailure.getCause(), is(failure));
        }
    }
    
    @Test
    void assertExecuteBatchWithoutMultiErrorHaltsAtFirstFailedMessage() throws SQLException {
        Connection connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        when(connection.getMetaData().getURL()).thenReturn("jdbc:firebirdsql://127.0.0.1/db");
        when(databaseConnectionManager.getConnections(any(), nullable(String.class), anyInt(), anyInt(), any(ConnectionMode.class))).thenReturn(Collections.singletonList(connection));
        PreparedStatement firstPreparedStatement = mock(PreparedStatement.class);
        PreparedStatement secondPreparedStatement = mock(PreparedStatement.class);
        when(firstPreparedStatement.getConnection()).thenReturn(connection);
        when(secondPreparedStatement.getConnection()).thenReturn(connection);
        BatchUpdateException failure = new BatchUpdateException("violation of PRIMARY or UNIQUE KEY constraint", "23000", 335544665, new int[0]);
        when(firstPreparedStatement.executeBatch()).thenThrow(failure);
        when(secondPreparedStatement.executeBatch()).thenReturn(new int[]{1});
        when(backendStatement.createStorageResource(any(ExecutionUnit.class), eq(connection), anyInt(), any(ConnectionMode.class), any(StatementOption.class), nullable(DatabaseType.class)))
                .thenReturn(firstPreparedStatement, secondPreparedStatement);
        ContextManager contextManager = mockContextManager("ds_0", "ds_1");
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdPreparedStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)",
                mockInsertStatementContext(), new HintValueContext());
        List<Object> firstParams = Arrays.asList(1, "foo_1");
        List<Object> secondParams = Arrays.asList(2, "foo_2");
        ExecutionUnit firstExecutionUnit = new ExecutionUnit("ds_0", new SQLUnit(firebirdPreparedStatement.getSql(), firstParams));
        ExecutionUnit secondExecutionUnit = new ExecutionUnit("ds_1", new SQLUnit(firebirdPreparedStatement.getSql(), secondParams));
        try (
                MockedConstruction<KernelProcessor> ignored = mockConstruction(KernelProcessor.class,
                        (mock, context) -> when(mock.generateExecutionContext(any(), any(), any()))
                                .thenReturn(new ExecutionContext(null, Collections.singletonList(firstExecutionUnit), mock(RouteContext.class)),
                                        new ExecutionContext(null, Collections.singletonList(secondExecutionUnit), mock(RouteContext.class))))) {
            FirebirdBatchCompletion actual = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdPreparedStatement,
                    Arrays.asList(firstParams, secondParams), false).executeBatch();
            verify(secondPreparedStatement, never()).executeBatch();
            verify(secondPreparedStatement).close();
            assertThat(actual.getRecordsCount(), is(1));
            assertThat(actual.getUpdateCounts(), is(new int[]{FirebirdBatchCompletion.EXECUTE_FAILED}));
            assertThat(actual.getFailures().size(), is(1));
            FirebirdBatchCompletion.Failure actualFailure = actual.getFailures().iterator().next();
            assertThat(actualFailure.getMessageIndex(), is(0));
            assertThat(actualFailure.getCause(), is(failure));
        }
    }
    
    @Test
    void assertExecuteBatchWithMultiErrorReportsMessagesAfterFailedRoute() throws SQLException {
        Connection connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        when(connection.getMetaData().getURL()).thenReturn("jdbc:firebirdsql://127.0.0.1/db");
        when(databaseConnectionManager.getConnections(any(), nullable(String.class), anyInt(), anyInt(), any(ConnectionMode.class))).thenReturn(Collections.singletonList(connection));
        PreparedStatement firstPreparedStatement = mock(PreparedStatement.class);
        PreparedStatement secondPreparedStatement = mock(PreparedStatement.class);
        when(firstPreparedStatement.getConnection()).thenReturn(connection);
        when(secondPreparedStatement.getConnection()).thenReturn(connection);
        BatchUpdateException failure = new BatchUpdateException("violation of PRIMARY or UNIQUE KEY constraint", "23000", 335544665, new int[0]);
        when(firstPreparedStatement.executeBatch()).thenThrow(failure);
        when(secondPreparedStatement.executeBatch()).thenReturn(new int[]{1});
        when(backendStatement.createStorageResource(any(ExecutionUnit.class), eq(connection), anyInt(), any(ConnectionMode.class), any(StatementOption.class), nullable(DatabaseType.class)))
                .thenReturn(firstPreparedStatement, secondPreparedStatement);
        ContextManager contextManager = mockContextManager("ds_0", "ds_1");
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdPreparedStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)",
                mockInsertStatementContext(), new HintValueContext());
        List<Object> firstParams = Arrays.asList(1, "foo_1");
        List<Object> secondParams = Arrays.asList(2, "foo_2");
        ExecutionUnit firstExecutionUnit = new ExecutionUnit("ds_0", new SQLUnit(firebirdPreparedStatement.getSql(), firstParams));
        ExecutionUnit secondExecutionUnit = new ExecutionUnit("ds_1", new SQLUnit(firebirdPreparedStatement.getSql(), secondParams));
        try (
                MockedConstruction<KernelProcessor> ignored = mockConstruction(KernelProcessor.class,
                        (mock, context) -> when(mock.generateExecutionContext(any(), any(), any()))
                                .thenReturn(new ExecutionContext(null, Collections.singletonList(firstExecutionUnit), mock(RouteContext.class)),
                                        new ExecutionContext(null, Collections.singletonList(secondExecutionUnit), mock(RouteContext.class))))) {
            FirebirdBatchCompletion actual = new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdPreparedStatement,
                    Arrays.asList(firstParams, secondParams), true).executeBatch();
            assertThat(actual.getRecordsCount(), is(2));
            assertThat(actual.getUpdateCounts(), is(new int[]{FirebirdBatchCompletion.EXECUTE_FAILED, 1}));
            assertThat(actual.getFailures().size(), is(1));
            FirebirdBatchCompletion.Failure actualFailure = actual.getFailures().iterator().next();
            assertThat(actualFailure.getMessageIndex(), is(0));
            assertThat(actualFailure.getCause(), is(failure));
        }
    }
    
    @Test
    void assertExecuteBatchWithoutMultiErrorStopsSameRouteAtFailedMessage() throws SQLException {
        PreparedStatement preparedStatement = mockSameRoutePreparedStatement();
        when(preparedStatement.executeBatch()).thenReturn(new int[]{1}).thenThrow(createFailure(new int[0]));
        FirebirdBatchCompletion actual = executeSameRouteBatch(false);
        verify(preparedStatement, times(2)).executeBatch();
        assertThat(actual.getRecordsCount(), is(2));
        assertThat(actual.getUpdateCounts(), is(new int[]{1, FirebirdBatchCompletion.EXECUTE_FAILED}));
        assertThat(actual.getFailures().size(), is(1));
        assertThat(actual.getFailures().iterator().next().getMessageIndex(), is(1));
    }
    
    @Test
    void assertExecuteBatchWithoutMultiErrorNeverSubmitsMessageAfterFailedMessageOnSameRoute() throws SQLException {
        PreparedStatement preparedStatement = mockSameRoutePreparedStatement();
        BatchUpdateException failure = createFailure(new int[0]);
        when(preparedStatement.executeBatch()).thenReturn(new int[]{1}).thenThrow(failure);
        FirebirdBatchCompletion actual = executeSameRouteBatch(false);
        assertThat(actual.getRecordsCount(), is(2));
        assertThat(actual.getUpdateCounts(), is(new int[]{1, FirebirdBatchCompletion.EXECUTE_FAILED}));
        assertThat(actual.getFailures().size(), is(1));
        FirebirdBatchCompletion.Failure actualFailure = actual.getFailures().iterator().next();
        assertThat(actualFailure.getMessageIndex(), is(1));
        assertThat(actualFailure.getCause(), is(failure));
        verify(preparedStatement, times(2)).addBatch();
        verify(preparedStatement, times(2)).executeBatch();
        verify(preparedStatement, never()).setObject(1, 3);
        verify(preparedStatement, never()).setObject(2, "foo_3");
    }
    
    @Test
    void assertExecuteBatchWithMultiErrorContinuesSameRouteAfterFailedMessage() throws SQLException {
        PreparedStatement preparedStatement = mockSameRoutePreparedStatement();
        when(preparedStatement.executeBatch()).thenThrow(createFailure(new int[]{1})).thenReturn(new int[]{1});
        FirebirdBatchCompletion actual = executeSameRouteBatch(true);
        assertThat(actual.getRecordsCount(), is(3));
        assertThat(actual.getUpdateCounts(), is(new int[]{1, FirebirdBatchCompletion.EXECUTE_FAILED, 1}));
        assertThat(actual.getFailures().size(), is(1));
        assertThat(actual.getFailures().iterator().next().getMessageIndex(), is(1));
        verify(preparedStatement, times(2)).executeBatch();
        verify(preparedStatement).clearBatch();
        verify(preparedStatement, times(4)).addBatch();
        verify(preparedStatement, times(2)).setObject(1, 3);
    }
    
    @Test
    void assertExecuteBatchWithNonBatchFailure() throws SQLException {
        PreparedStatement preparedStatement = mockSameRoutePreparedStatement();
        SQLException failure = new SQLException("connection lost", "08006", 335544721);
        when(preparedStatement.executeBatch()).thenThrow(failure);
        FirebirdBatchCompletion actual = executeSameRouteBatch(false);
        assertThat(actual.getRecordsCount(), is(1));
        assertThat(actual.getUpdateCounts(), is(new int[]{FirebirdBatchCompletion.EXECUTE_FAILED}));
        assertThat(actual.getFailures().size(), is(1));
        FirebirdBatchCompletion.Failure actualFailure = actual.getFailures().iterator().next();
        assertThat(actualFailure.getMessageIndex(), is(0));
        assertThat(actualFailure.getCause(), is(failure));
    }
    
    @Test
    void assertExecuteBatchWithMultiErrorReportsChainedCausePerMessage() throws SQLException {
        PreparedStatement preparedStatement = mockSameRoutePreparedStatement();
        BatchUpdateException firstFailure = createFailure(new int[]{1});
        SQLException firstCause = new SQLException("first violation", "23000", 335544665);
        firstFailure.setNextException(firstCause);
        BatchUpdateException secondFailure = createFailure(new int[0]);
        SQLException secondCause = new SQLException("second violation", "23000", 335544349);
        secondFailure.setNextException(secondCause);
        when(preparedStatement.executeBatch()).thenThrow(firstFailure).thenThrow(secondFailure);
        FirebirdBatchCompletion actual = executeSameRouteBatch(true);
        assertThat(actual.getRecordsCount(), is(3));
        assertThat(actual.getUpdateCounts(), is(new int[]{1, FirebirdBatchCompletion.EXECUTE_FAILED, FirebirdBatchCompletion.EXECUTE_FAILED}));
        Iterator<FirebirdBatchCompletion.Failure> actualFailures = actual.getFailures().iterator();
        FirebirdBatchCompletion.Failure actualFirstFailure = actualFailures.next();
        assertThat(actualFirstFailure.getMessageIndex(), is(1));
        assertThat(actualFirstFailure.getCause(), is(firstCause));
        FirebirdBatchCompletion.Failure actualSecondFailure = actualFailures.next();
        assertThat(actualSecondFailure.getMessageIndex(), is(2));
        assertThat(actualSecondFailure.getCause(), is(secondCause));
    }
    
    @Test
    void assertExecuteBatchWithInterleavedRoutesStopsBeforeLaterMessageOfEarlierRoute() throws SQLException {
        PreparedStatement firstPreparedStatement = mock(PreparedStatement.class);
        PreparedStatement secondPreparedStatement = mock(PreparedStatement.class);
        BatchUpdateException failure = createFailure(new int[0]);
        when(firstPreparedStatement.executeBatch()).thenReturn(new int[]{1});
        when(secondPreparedStatement.executeBatch()).thenThrow(failure);
        FirebirdBatchCompletion actual = executeInterleavedRoutesBatch(firstPreparedStatement, secondPreparedStatement);
        assertThat(actual.getRecordsCount(), is(2));
        assertThat(actual.getUpdateCounts(), is(new int[]{1, FirebirdBatchCompletion.EXECUTE_FAILED}));
        assertThat(actual.getFailures().size(), is(1));
        FirebirdBatchCompletion.Failure actualFailure = actual.getFailures().iterator().next();
        assertThat(actualFailure.getMessageIndex(), is(1));
        assertThat(actualFailure.getCause(), is(failure));
        InOrder inOrder = inOrder(firstPreparedStatement, secondPreparedStatement);
        inOrder.verify(firstPreparedStatement).setObject(1, 1);
        inOrder.verify(firstPreparedStatement).addBatch();
        inOrder.verify(firstPreparedStatement).executeBatch();
        inOrder.verify(secondPreparedStatement).setObject(1, 2);
        inOrder.verify(secondPreparedStatement).addBatch();
        inOrder.verify(secondPreparedStatement).executeBatch();
        verify(firstPreparedStatement).addBatch();
        verify(secondPreparedStatement).addBatch();
        verify(firstPreparedStatement, never()).setObject(1, 3);
        verify(firstPreparedStatement).executeBatch();
        verify(secondPreparedStatement).executeBatch();
    }
    
    @Test
    void assertExecuteBatchWithInterleavedRoutesReportsFailureOfLastMessage() throws SQLException {
        PreparedStatement firstPreparedStatement = mock(PreparedStatement.class);
        PreparedStatement secondPreparedStatement = mock(PreparedStatement.class);
        BatchUpdateException failure = createFailure(new int[0]);
        when(firstPreparedStatement.executeBatch()).thenReturn(new int[]{1}).thenThrow(failure);
        when(secondPreparedStatement.executeBatch()).thenReturn(new int[]{1});
        FirebirdBatchCompletion actual = executeInterleavedRoutesBatch(firstPreparedStatement, secondPreparedStatement);
        assertThat(actual.getRecordsCount(), is(3));
        assertThat(actual.getUpdateCounts(), is(new int[]{1, 1, FirebirdBatchCompletion.EXECUTE_FAILED}));
        assertThat(actual.getFailures().size(), is(1));
        FirebirdBatchCompletion.Failure actualFailure = actual.getFailures().iterator().next();
        assertThat(actualFailure.getMessageIndex(), is(2));
        assertThat(actualFailure.getCause(), is(failure));
        InOrder inOrder = inOrder(firstPreparedStatement, secondPreparedStatement);
        inOrder.verify(firstPreparedStatement).setObject(1, 1);
        inOrder.verify(firstPreparedStatement).addBatch();
        inOrder.verify(firstPreparedStatement).executeBatch();
        inOrder.verify(secondPreparedStatement).setObject(1, 2);
        inOrder.verify(secondPreparedStatement).addBatch();
        inOrder.verify(secondPreparedStatement).executeBatch();
        inOrder.verify(firstPreparedStatement).setObject(1, 3);
        inOrder.verify(firstPreparedStatement).addBatch();
        inOrder.verify(firstPreparedStatement).executeBatch();
        verify(firstPreparedStatement).setObject(1, 3);
        verify(firstPreparedStatement, times(2)).addBatch();
        verify(secondPreparedStatement).addBatch();
        verify(firstPreparedStatement, times(2)).executeBatch();
        verify(secondPreparedStatement).executeBatch();
    }
    
    private FirebirdBatchCompletion executeInterleavedRoutesBatch(final PreparedStatement firstPreparedStatement, final PreparedStatement secondPreparedStatement) throws SQLException {
        Connection connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        when(connection.getMetaData().getURL()).thenReturn("jdbc:firebirdsql://127.0.0.1/db");
        when(databaseConnectionManager.getConnections(any(), nullable(String.class), anyInt(), anyInt(), any(ConnectionMode.class))).thenReturn(Collections.singletonList(connection));
        when(firstPreparedStatement.getConnection()).thenReturn(connection);
        when(secondPreparedStatement.getConnection()).thenReturn(connection);
        when(backendStatement.createStorageResource(any(ExecutionUnit.class), eq(connection), anyInt(), any(ConnectionMode.class), any(StatementOption.class), nullable(DatabaseType.class)))
                .thenReturn(firstPreparedStatement, secondPreparedStatement);
        ContextManager contextManager = mockContextManager("ds_0", "ds_1");
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdPreparedStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)",
                mockInsertStatementContext(), new HintValueContext());
        List<List<Object>> parameterSets = Arrays.asList(Arrays.asList(1, "foo_1"), Arrays.asList(2, "foo_2"), Arrays.asList(3, "foo_3"));
        ExecutionUnit firstExecutionUnit = new ExecutionUnit("ds_0", new SQLUnit("INSERT INTO t_0 (id, col) VALUES (?, ?)", parameterSets.get(0)));
        ExecutionUnit secondExecutionUnit = new ExecutionUnit("ds_1", new SQLUnit("INSERT INTO t_1 (id, col) VALUES (?, ?)", parameterSets.get(1)));
        ExecutionUnit thirdExecutionUnit = new ExecutionUnit("ds_0", new SQLUnit("INSERT INTO t_0 (id, col) VALUES (?, ?)", parameterSets.get(2)));
        try (
                MockedConstruction<KernelProcessor> ignored = mockConstruction(KernelProcessor.class,
                        (mock, context) -> when(mock.generateExecutionContext(any(), any(), any()))
                                .thenReturn(new ExecutionContext(null, Collections.singletonList(firstExecutionUnit), mock(RouteContext.class)),
                                        new ExecutionContext(null, Collections.singletonList(secondExecutionUnit), mock(RouteContext.class)),
                                        new ExecutionContext(null, Collections.singletonList(thirdExecutionUnit), mock(RouteContext.class))))) {
            return new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdPreparedStatement, parameterSets, false).executeBatch();
        }
    }
    
    private BatchUpdateException createFailure(final int[] updateCounts) {
        return new BatchUpdateException("violation of PRIMARY or UNIQUE KEY constraint", "23000", 335544665, updateCounts);
    }
    
    private PreparedStatement mockSameRoutePreparedStatement() throws SQLException {
        Connection connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        when(connection.getMetaData().getURL()).thenReturn("jdbc:firebirdsql://127.0.0.1/db");
        when(databaseConnectionManager.getConnections(any(), nullable(String.class), anyInt(), anyInt(), any(ConnectionMode.class))).thenReturn(Collections.singletonList(connection));
        PreparedStatement result = mock(PreparedStatement.class);
        when(result.getConnection()).thenReturn(connection);
        when(backendStatement.createStorageResource(any(ExecutionUnit.class), eq(connection), anyInt(), any(ConnectionMode.class), any(StatementOption.class), nullable(DatabaseType.class)))
                .thenReturn(result);
        return result;
    }
    
    private FirebirdBatchCompletion executeSameRouteBatch(final boolean multiError) throws SQLException {
        ContextManager contextManager = mockContextManager();
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        FirebirdServerPreparedStatement firebirdPreparedStatement = new FirebirdServerPreparedStatement("INSERT INTO t (id, col) VALUES (?, ?)",
                mockInsertStatementContext(), new HintValueContext());
        List<List<Object>> parameterSets = Arrays.asList(Arrays.asList(1, "foo_1"), Arrays.asList(2, "foo_2"), Arrays.asList(3, "foo_3"));
        return new FirebirdBatchedStatementsExecutor(mockConnectionSession(), firebirdPreparedStatement, parameterSets, multiError).executeBatch();
    }
    
    private InsertStatementContext mockInsertStatementContext() {
        InsertStatement insertStatement = InsertStatement.builder()
                .databaseType(databaseType)
                .table(new SimpleTableSegment(new TableNameSegment(0, 0, new IdentifierValue("t"))))
                .insertColumns(new InsertColumnsSegment(0, 0, Arrays.asList(
                        new ColumnSegment(0, 0, new IdentifierValue("id")), new ColumnSegment(0, 0, new IdentifierValue("col")))))
                .values(Collections.singleton(new InsertValuesSegment(0, 0, Arrays.asList(
                        new ParameterMarkerExpressionSegment(0, 0, 0), new ParameterMarkerExpressionSegment(0, 0, 1)))))
                .build();
        InsertStatementContext result = mock(InsertStatementContext.class);
        when(result.getSqlStatement()).thenReturn(insertStatement);
        return result;
    }
    
    private ContextManager mockContextManager() {
        return mockContextManager("ds_0");
    }
    
    private ContextManager mockContextManager(final String... dataSourceNames) {
        ShardingSphereDatabase database = mock(ShardingSphereDatabase.class, RETURNS_DEEP_STUBS);
        Map<String, StorageUnit> storageUnits = new LinkedHashMap<>(dataSourceNames.length, 1F);
        for (String each : dataSourceNames) {
            StorageUnit storageUnit = mock(StorageUnit.class, RETURNS_DEEP_STUBS);
            when(storageUnit.getStorageType()).thenReturn(databaseType);
            storageUnits.put(each, storageUnit);
        }
        when(database.getProtocolType()).thenReturn(databaseType);
        when(database.getDefaultSchemaName()).thenReturn("DB");
        when(database.getResourceMetaData().getStorageUnits()).thenReturn(storageUnits);
        when(database.getResourceMetaData().getAllInstanceDataSourceNames()).thenReturn(Arrays.asList(dataSourceNames));
        when(database.getRuleMetaData()).thenReturn(new RuleMetaData(Collections.emptyList()));
        when(database.containsSchema("public")).thenReturn(true);
        when(database.containsSchema(new IdentifierValue("public"))).thenReturn(true);
        when(database.containsSchema("DB")).thenReturn(true);
        when(database.containsSchema(new IdentifierValue("DB"))).thenReturn(true);
        ShardingSphereSchema schema = mock(ShardingSphereSchema.class, RETURNS_DEEP_STUBS);
        when(database.getSchema("public")).thenReturn(schema);
        when(database.getSchema(new IdentifierValue("public"))).thenReturn(schema);
        when(database.getSchema("DB")).thenReturn(schema);
        when(database.getSchema(new IdentifierValue("DB"))).thenReturn(schema);
        when(schema.containsTable("t")).thenReturn(true);
        when(schema.containsTable(new IdentifierValue("t"))).thenReturn(true);
        when(schema.getTable("t").getAllColumns()).thenReturn(Arrays.asList(new ShardingSphereColumn("id", Types.INTEGER, false, false, false, true, false, false),
                new ShardingSphereColumn("col", Types.VARCHAR, false, false, false, true, false, false)));
        when(schema.getTable(new IdentifierValue("t")).getAllColumns()).thenReturn(Arrays.asList(new ShardingSphereColumn("id", Types.INTEGER, false, false, false, true, false, false),
                new ShardingSphereColumn("col", Types.VARCHAR, false, false, false, true, false, false)));
        ContextManager result = mock(ContextManager.class, RETURNS_DEEP_STUBS);
        when(result.getMetaDataContexts().getMetaData().containsDatabase("db")).thenReturn(true);
        when(result.getMetaDataContexts().getMetaData().containsDatabase(new IdentifierValue("db"))).thenReturn(true);
        when(result.getMetaDataContexts().getMetaData().getDatabase("db")).thenReturn(database);
        when(result.getMetaDataContexts().getMetaData().getDatabase(new IdentifierValue("db"))).thenReturn(database);
        RuleMetaData globalRuleMetaData = new RuleMetaData(Arrays.asList(new SQLTranslatorRule(new DefaultSQLTranslatorRuleConfigurationBuilder().build()),
                new SQLParserRule(new DefaultSQLParserRuleConfigurationBuilder().build())));
        when(result.getMetaDataContexts().getMetaData().getGlobalRuleMetaData()).thenReturn(globalRuleMetaData);
        when(result.getMetaDataContexts().getMetaData().getProps()).thenReturn(new ConfigurationProperties(new Properties()));
        return result;
    }
    
    private ConnectionSession mockConnectionSession() {
        ConnectionSession result = mock(ConnectionSession.class);
        when(result.getCurrentDatabaseName()).thenReturn("db");
        when(result.getUsedDatabaseName()).thenReturn("db");
        when(result.getDatabaseConnectionManager()).thenReturn(databaseConnectionManager);
        when(result.getStatementManager()).thenReturn(backendStatement);
        ConnectionContext connectionContext = new ConnectionContext(Collections::emptySet);
        connectionContext.setCurrentDatabaseName("db");
        when(result.getConnectionContext()).thenReturn(connectionContext);
        return result;
    }
}
