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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query.statement.prepare;

import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.database.connector.firebird.metadata.data.FirebirdBlobInfoRegistry;
import org.apache.shardingsphere.database.exception.core.exception.syntax.database.NoDatabaseSelectedException;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.FirebirdBinaryColumnType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.type.sql.FirebirdSQLInfoPacketType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.type.sql.FirebirdSQLInfoReturnValue;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.prepare.FirebirdPrepareStatementPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.prepare.FirebirdPrepareStatementReturnPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.prepare.FirebirdReturnColumnPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.generic.FirebirdGenericResponsePacket;
import org.apache.shardingsphere.database.protocol.firebird.payload.FirebirdPacketPayload;
import org.apache.shardingsphere.database.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.infra.binder.context.statement.type.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.hint.HintValueContext;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.resource.ResourceMetaData;
import org.apache.shardingsphere.infra.metadata.database.rule.RuleMetaData;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereColumn;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereTable;
import org.apache.shardingsphere.infra.metadata.statistics.ShardingSphereStatistics;
import org.apache.shardingsphere.infra.metadata.user.Grantee;
import org.apache.shardingsphere.infra.session.connection.ConnectionContext;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.mode.metadata.MetaDataContexts;
import org.apache.shardingsphere.parser.config.SQLParserRuleConfiguration;
import org.apache.shardingsphere.parser.rule.SQLParserRule;
import org.apache.shardingsphere.proxy.backend.connector.ProxyDatabaseConnectionManager;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.handler.ProxyBackendHandler;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.backend.session.ServerPreparedStatementRegistry;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.FirebirdServerPreparedStatement;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.statement.FirebirdStatementResourceCleaner;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.statement.fetch.FirebirdFetchStatementCache;
import org.apache.shardingsphere.sql.parser.engine.api.CacheOption;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.ddl.database.DropDatabaseStatement;
import org.apache.shardingsphere.test.infra.framework.extension.mock.AutoMockExtension;
import org.apache.shardingsphere.test.infra.framework.extension.mock.StaticMockSettings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(AutoMockExtension.class)
@StaticMockSettings(ProxyContext.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class FirebirdPrepareStatementCommandExecutorTest {
    
    private static final int CONNECTION_ID = 1;
    
    private final DatabaseType databaseType = TypedSPILoader.getService(DatabaseType.class, "Firebird");
    
    @Mock
    private FirebirdPrepareStatementPacket packet;
    
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConnectionSession connectionSession;
    
    @Mock
    private ProxyDatabaseConnectionManager connectionManager;
    
    @Mock
    private ProxyBackendHandler proxyBackendHandler;
    
    @Mock
    private ConnectionContext connectionContext;
    
    @BeforeEach
    void setUp() {
        FirebirdFetchStatementCache.getInstance().registerConnection(CONNECTION_ID);
        when(connectionSession.getServerPreparedStatementRegistry()).thenReturn(new ServerPreparedStatementRegistry());
        when(connectionSession.getCurrentDatabaseName()).thenReturn("foo_db");
        when(connectionSession.getConnectionContext()).thenReturn(connectionContext);
        when(connectionContext.getGrantee()).thenReturn(new Grantee("foo_user"));
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        when(connectionSession.getDatabaseConnectionManager()).thenReturn(connectionManager);
        when(packet.getSQL()).thenReturn("SELECT 1");
        when(packet.getHintValueContext()).thenReturn(new HintValueContext());
        when(packet.isValidStatementHandle()).thenReturn(true);
        when(packet.getStatementId()).thenReturn(1);
        when(packet.nextItem()).thenReturn(true, false);
        when(packet.getCurrentItem()).thenReturn(FirebirdSQLInfoPacketType.STMT_TYPE);
        MetaDataContexts metaDataContexts = createMetaDataContexts();
        when(ProxyContext.getInstance().getContextManager().getMetaDataContexts()).thenReturn(metaDataContexts);
    }
    
    @AfterEach
    void tearDown() {
        FirebirdFetchStatementCache.getInstance().unregisterStatement(CONNECTION_ID, 1);
        FirebirdFetchStatementCache.getInstance().unregisterConnection(CONNECTION_ID);
        FirebirdBlobInfoRegistry.refreshTable("foo_db", "foo_tbl", Collections.emptyMap());
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("describeExpressionParametersArguments")
    void assertDescribeExpressionParameters(final String name, final String sql, final Collection<FirebirdBinaryColumnType> expectedTypes) throws Exception {
        FirebirdPrepareStatementReturnPacket returnPacket = prepareWithDescribeBind(sql);
        assertThat(returnPacket.getDescribeBind().size(), is(expectedTypes.size()));
        int index = 0;
        for (FirebirdBinaryColumnType each : expectedTypes) {
            FirebirdPacketPayload payload = mock(FirebirdPacketPayload.class, RETURNS_DEEP_STUBS);
            returnPacket.getDescribeBind().get(index++).write(payload);
            ArgumentCaptor<Integer> argumentCaptor = ArgumentCaptor.forClass(Integer.class);
            verify(payload, atLeastOnce()).writeInt4LE(argumentCaptor.capture());
            assertThat(argumentCaptor.getAllValues().get(0) & ~1, is(each.getValue()));
        }
    }
    
    private static Stream<Arguments> describeExpressionParametersArguments() {
        return Stream.of(
                Arguments.of("insert_cast_and_plain_values", "INSERT INTO foo_tbl (id, content) VALUES (CAST(? AS INTEGER), ?)",
                        Arrays.asList(FirebirdBinaryColumnType.LONG, FirebirdBinaryColumnType.BLOB)),
                Arguments.of("insert_expression_values", "INSERT INTO foo_tbl (id, content) VALUES (1 / ?, UPPER(?) || ?)",
                        Arrays.asList(FirebirdBinaryColumnType.LONG, FirebirdBinaryColumnType.BLOB, FirebirdBinaryColumnType.BLOB)),
                Arguments.of("where_cast_before_column", "SELECT id FROM foo_tbl WHERE id = CAST(? AS SMALLINT) AND content = ?",
                        Arrays.asList(FirebirdBinaryColumnType.SHORT, FirebirdBinaryColumnType.BLOB)),
                Arguments.of("where_arithmetic_between_in", "SELECT id FROM foo_tbl WHERE ? = id and id = 1 / ? AND id BETWEEN ? AND ? OR id IN (?, ?)",
                        Collections.nCopies(6, FirebirdBinaryColumnType.LONG)),
                Arguments.of("update_set_expression", "UPDATE foo_tbl SET id = id + ? WHERE id = ABS(?)", Arrays.asList(FirebirdBinaryColumnType.LONG, FirebirdBinaryColumnType.DOUBLE)),
                Arguments.of("delete_cast_bigint_and_date", "DELETE FROM foo_tbl WHERE id = CAST(? AS BIGINT) OR CAST(? AS DATE) IS NULL",
                        Arrays.asList(FirebirdBinaryColumnType.INT64, FirebirdBinaryColumnType.DATE)),
                Arguments.of("untyped", "SELECT id FROM foo_tbl WHERE ? = ?", Arrays.asList(FirebirdBinaryColumnType.VARYING, FirebirdBinaryColumnType.VARYING)));
    }
    
    @Test
    void assertDescribeTextParameterLength() throws Exception {
        FirebirdPrepareStatementReturnPacket returnPacket = prepareWithDescribeBind("SELECT id FROM foo_tbl WHERE id = CAST(? AS VARCHAR(10)) AND ? = ?");
        assertThat(returnPacket.getDescribeBind().size(), is(3));
        int[] expectedLengths = {10, 512, 512};
        for (int i = 0; i < expectedLengths.length; i++) {
            FirebirdPacketPayload payload = mock(FirebirdPacketPayload.class, RETURNS_DEEP_STUBS);
            returnPacket.getDescribeBind().get(i).write(payload);
            verify(payload).writeInt4LE(expectedLengths[i]);
        }
    }
    
    private FirebirdPrepareStatementReturnPacket prepareWithDescribeBind(final String sql) throws Exception {
        when(packet.getSQL()).thenReturn(sql);
        when(packet.nextItem()).thenReturn(true, true, true, true, true, true, false);
        when(packet.getCurrentItem()).thenReturn(FirebirdSQLInfoPacketType.STMT_TYPE, FirebirdSQLInfoPacketType.BIND, FirebirdSQLInfoPacketType.TYPE, FirebirdSQLInfoPacketType.TYPE,
                FirebirdSQLInfoPacketType.LENGTH, FirebirdSQLInfoPacketType.LENGTH, FirebirdSQLInfoPacketType.DESCRIBE_END, FirebirdSQLInfoPacketType.DESCRIBE_END);
        return (FirebirdPrepareStatementReturnPacket) ((FirebirdGenericResponsePacket) new FirebirdPrepareStatementCommandExecutor(packet, connectionSession).execute().iterator().next()).getData();
    }
    
    private MetaDataContexts createMetaDataContexts() {
        SQLParserRule parserRule = new SQLParserRule(new SQLParserRuleConfiguration(new CacheOption(128, 1024L), new CacheOption(128, 1024L)));
        RuleMetaData globalRuleMetaData = new RuleMetaData(Collections.singleton(parserRule));
        ShardingSphereColumn column = new ShardingSphereColumn("id", Types.INTEGER, false, false, true, true, false, true);
        ShardingSphereColumn blobColumn = new ShardingSphereColumn("content", Types.BLOB, false, false, true, true, false, true);
        ShardingSphereTable table = new ShardingSphereTable("foo_tbl", Arrays.asList(column, blobColumn), Collections.emptyList(), Collections.emptyList());
        ShardingSphereSchema schema = new ShardingSphereSchema("foo_db", databaseType, Collections.singleton(table), Collections.emptyList());
        ShardingSphereDatabase database = spy(new ShardingSphereDatabase(
                "foo_db", databaseType, new ResourceMetaData(Collections.emptyMap()), new RuleMetaData(Collections.emptyList()), Collections.singleton(schema),
                new ConfigurationProperties(new Properties())));
        doReturn(Optional.of(schema)).when(database).findDefaultSchema();
        doReturn(null).when(database).getSchema("FOO_DB");
        ShardingSphereMetaData metaData = new ShardingSphereMetaData(
                Collections.singleton(database), new ResourceMetaData(Collections.emptyMap()), globalRuleMetaData, new ConfigurationProperties(new Properties()));
        return new MetaDataContexts(metaData, new ShardingSphereStatistics());
    }
    
    @Test
    void assertExecute() throws Exception {
        FirebirdPrepareStatementCommandExecutor executor = new FirebirdPrepareStatementCommandExecutor(packet, connectionSession);
        Collection<DatabasePacket> actual = executor.execute();
        FirebirdGenericResponsePacket responsePacket = (FirebirdGenericResponsePacket) actual.iterator().next();
        FirebirdPrepareStatementReturnPacket returnPacket = (FirebirdPrepareStatementReturnPacket) responsePacket.getData();
        assertThat(returnPacket.getType(), is(FirebirdSQLInfoReturnValue.SELECT));
        FirebirdServerPreparedStatement preparedStatement = connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(1);
        assertThat(preparedStatement.getSql(), is("SELECT 1"));
        assertThat(preparedStatement.getSqlStatementContext(), isA(SelectStatementContext.class));
    }
    
    @Test
    void assertExecuteDropDatabaseWithoutNameResolvesCurrentDatabase() throws Exception {
        when(packet.getSQL()).thenReturn("DROP DATABASE");
        when(connectionSession.getUsedDatabaseName()).thenReturn("foo_db");
        FirebirdPrepareStatementCommandExecutor executor = new FirebirdPrepareStatementCommandExecutor(packet, connectionSession);
        Collection<DatabasePacket> actual = executor.execute();
        FirebirdGenericResponsePacket responsePacket = (FirebirdGenericResponsePacket) actual.iterator().next();
        FirebirdPrepareStatementReturnPacket returnPacket = (FirebirdPrepareStatementReturnPacket) responsePacket.getData();
        assertThat(returnPacket.getType(), is(FirebirdSQLInfoReturnValue.DDL));
        FirebirdServerPreparedStatement preparedStatement = connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(1);
        assertThat(((DropDatabaseStatement) preparedStatement.getSqlStatementContext().getSqlStatement()).getDatabaseName(), is("foo_db"));
    }
    
    @Test
    void assertExecuteDropDatabaseWithoutNameAndCurrentDatabase() {
        when(packet.getSQL()).thenReturn("DROP DATABASE");
        when(connectionSession.getUsedDatabaseName()).thenReturn(null);
        FirebirdPrepareStatementCommandExecutor executor = new FirebirdPrepareStatementCommandExecutor(packet, connectionSession);
        assertThrows(NoDatabaseSelectedException.class, executor::execute);
    }
    
    @Test
    void assertDescribeCountReturnsBigintType() throws Exception {
        when(packet.getSQL()).thenReturn("SELECT COUNT(*) FROM foo_tbl");
        when(packet.nextItem()).thenReturn(true, true, true, true, true, false);
        when(packet.getCurrentItem()).thenReturn(
                FirebirdSQLInfoPacketType.STMT_TYPE,
                FirebirdSQLInfoPacketType.SELECT,
                FirebirdSQLInfoPacketType.TYPE,
                FirebirdSQLInfoPacketType.TYPE,
                FirebirdSQLInfoPacketType.DESCRIBE_END,
                FirebirdSQLInfoPacketType.DESCRIBE_END);
        FirebirdPrepareStatementCommandExecutor executor = new FirebirdPrepareStatementCommandExecutor(packet, connectionSession);
        Collection<DatabasePacket> actual = executor.execute();
        FirebirdGenericResponsePacket responsePacket = (FirebirdGenericResponsePacket) actual.iterator().next();
        FirebirdPrepareStatementReturnPacket returnPacket = (FirebirdPrepareStatementReturnPacket) responsePacket.getData();
        assertThat(returnPacket.getDescribeSelect().size(), is(1));
        FirebirdPacketPayload payload = mock(FirebirdPacketPayload.class, RETURNS_DEEP_STUBS);
        FirebirdReturnColumnPacket columnPacket = returnPacket.getDescribeSelect().get(0);
        columnPacket.write(payload);
        verify(payload).writeInt4LE(FirebirdBinaryColumnType.INT64.getValue() + 1);
    }
    
    @Test
    void assertDescribeBlobColumnRegisteredInBlobInfoRegistryReturnsBlobTypeAndSubtype() throws Exception {
        FirebirdBlobInfoRegistry.refreshTable("foo_db", "foo_tbl", Collections.singletonMap("id", 7));
        FirebirdReturnColumnPacket columnPacket = describeSingleColumn("SELECT id FROM foo_tbl");
        FirebirdPacketPayload payload = mock(FirebirdPacketPayload.class, RETURNS_DEEP_STUBS);
        columnPacket.write(payload);
        verify(payload).writeInt4LE(FirebirdBinaryColumnType.BLOB.getValue() + 1);
        verify(payload).writeInt4LE(7);
    }
    
    @Test
    void assertDescribeBlobTypeColumnWithoutRegistryEntryReturnsBlobTypeAndDefaultSubtype() throws Exception {
        FirebirdReturnColumnPacket columnPacket = describeSingleColumn("SELECT content FROM foo_tbl");
        FirebirdPacketPayload payload = mock(FirebirdPacketPayload.class, RETURNS_DEEP_STUBS);
        columnPacket.write(payload);
        verify(payload).writeInt4LE(FirebirdBinaryColumnType.BLOB.getValue() + 1);
        verify(payload).writeInt4LE(FirebirdBinaryColumnType.BLOB.getSubtype());
    }
    
    @Test
    void assertDescribeDerivedTableSelect() throws Exception {
        when(packet.getSQL()).thenReturn("SELECT * FROM (SELECT id FROM foo_tbl) derived_tbl");
        when(packet.nextItem()).thenReturn(true, true, true, true, true, false);
        when(packet.getCurrentItem()).thenReturn(
                FirebirdSQLInfoPacketType.STMT_TYPE,
                FirebirdSQLInfoPacketType.SELECT,
                FirebirdSQLInfoPacketType.TYPE,
                FirebirdSQLInfoPacketType.TYPE,
                FirebirdSQLInfoPacketType.DESCRIBE_END,
                FirebirdSQLInfoPacketType.DESCRIBE_END);
        FirebirdPrepareStatementCommandExecutor executor = new FirebirdPrepareStatementCommandExecutor(packet, connectionSession);
        Collection<DatabasePacket> actual = executor.execute();
        FirebirdGenericResponsePacket responsePacket = (FirebirdGenericResponsePacket) actual.iterator().next();
        FirebirdPrepareStatementReturnPacket returnPacket = (FirebirdPrepareStatementReturnPacket) responsePacket.getData();
        assertThat(returnPacket.getType(), is(FirebirdSQLInfoReturnValue.SELECT));
        assertThat(returnPacket.getDescribeSelect().size(), is(1));
        FirebirdPacketPayload payload = mock(FirebirdPacketPayload.class, RETURNS_DEEP_STUBS);
        FirebirdReturnColumnPacket columnPacket = returnPacket.getDescribeSelect().get(0);
        columnPacket.write(payload);
        verify(payload).writeInt4LE(FirebirdBinaryColumnType.LONG.getValue() + 1);
    }
    
    private FirebirdReturnColumnPacket describeSingleColumn(final String sql) throws Exception {
        when(packet.getSQL()).thenReturn(sql);
        when(packet.nextItem()).thenReturn(true, true, true, true, true, true, false);
        when(packet.getCurrentItem()).thenReturn(
                FirebirdSQLInfoPacketType.STMT_TYPE,
                FirebirdSQLInfoPacketType.SELECT,
                FirebirdSQLInfoPacketType.TYPE,
                FirebirdSQLInfoPacketType.TYPE,
                FirebirdSQLInfoPacketType.SUB_TYPE,
                FirebirdSQLInfoPacketType.SUB_TYPE,
                FirebirdSQLInfoPacketType.DESCRIBE_END,
                FirebirdSQLInfoPacketType.DESCRIBE_END);
        FirebirdPrepareStatementCommandExecutor executor = new FirebirdPrepareStatementCommandExecutor(packet, connectionSession);
        Collection<DatabasePacket> actual = executor.execute();
        FirebirdGenericResponsePacket responsePacket = (FirebirdGenericResponsePacket) actual.iterator().next();
        FirebirdPrepareStatementReturnPacket returnPacket = (FirebirdPrepareStatementReturnPacket) responsePacket.getData();
        assertThat(returnPacket.getDescribeSelect().size(), is(1));
        return returnPacket.getDescribeSelect().get(0);
    }
    
    @Test
    void assertExecuteWithValidStatementHandleCleansPreviousPreparedStatementResources() throws Exception {
        connectionSession.getServerPreparedStatementRegistry().addPreparedStatement(1, new FirebirdServerPreparedStatement("SELECT 0", mock(SelectStatementContext.class), new HintValueContext()));
        FirebirdFetchStatementCache.getInstance().registerStatement(CONNECTION_ID, 1, proxyBackendHandler);
        FirebirdPrepareStatementCommandExecutor executor = new FirebirdPrepareStatementCommandExecutor(packet, connectionSession);
        executor.execute();
        verify(connectionSession).invalidatePreparedStatementCache(FirebirdStatementResourceCleaner.createPreparedStatementCacheKey(1));
        assertThat(connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(1).getSql(), is("SELECT 1"));
        assertThat(FirebirdFetchStatementCache.getInstance().getFetchBackendHandler(CONNECTION_ID, 1), is((ProxyBackendHandler) null));
    }
    
    @Test
    void assertExecuteWithValidStatementHandleWithoutFetchHandler() throws Exception {
        FirebirdPrepareStatementCommandExecutor executor = new FirebirdPrepareStatementCommandExecutor(packet, connectionSession);
        executor.execute();
        verify(connectionSession).invalidatePreparedStatementCache(FirebirdStatementResourceCleaner.createPreparedStatementCacheKey(1));
    }
}
