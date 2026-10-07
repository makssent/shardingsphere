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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query.statement;

import org.apache.shardingsphere.database.connector.firebird.metadata.data.FirebirdNumericColumn;
import org.apache.shardingsphere.database.protocol.binary.BinaryCell;
import org.apache.shardingsphere.database.protocol.binary.BinaryRow;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.FirebirdBinaryColumnType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.prepare.FirebirdReturnColumnPacket;
import org.apache.shardingsphere.proxy.backend.response.data.QueryResponseCell;
import org.apache.shardingsphere.proxy.backend.response.data.QueryResponseRow;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.Arrays;
import java.util.Iterator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FirebirdBinaryRowBuilderTest {
    
    @Test
    void assertBuildPreservesCellOrderAndData() {
        byte[] expectedData = new byte[]{1, 2};
        QueryResponseRow row = new QueryResponseRow(Arrays.asList(new QueryResponseCell(Types.BLOB, expectedData), new QueryResponseCell(Types.INTEGER, null)));
        BinaryRow actual = FirebirdBinaryRowBuilder.build(row);
        assertThat(actual.getCells().size(), is(2));
        Iterator<BinaryCell> iterator = actual.getCells().iterator();
        BinaryCell firstCell = iterator.next();
        assertThat(firstCell.getColumnType(), is(FirebirdBinaryColumnType.BLOB));
        assertThat(firstCell.getData(), sameInstance(expectedData));
        BinaryCell secondCell = iterator.next();
        assertThat(secondCell.getColumnType(), is(FirebirdBinaryColumnType.LONG));
        assertNull(secondCell.getData());
        assertFalse(iterator.hasNext());
    }
    
    @Test
    void assertBuildNumericColumnsWithDescribedStorageTypeAndScale() {
        QueryResponseRow row = new QueryResponseRow(Arrays.asList(new QueryResponseCell(Types.NUMERIC, new BigDecimal("-12.34")), new QueryResponseCell(Types.DECIMAL, new BigDecimal("123456.789")),
                new QueryResponseCell(Types.NUMERIC, null), new QueryResponseCell(Types.INTEGER, 7)));
        BinaryRow actual = FirebirdBinaryRowBuilder.build(row, Arrays.asList(
                mockColumn(new FirebirdNumericColumn(7, 1, -2)), mockColumn(new FirebirdNumericColumn(8, 2, -3)), mockColumn(new FirebirdNumericColumn(16, 2, -4)), mockColumn(null)));
        Iterator<BinaryCell> iterator = actual.getCells().iterator();
        BinaryCell smallint = iterator.next();
        assertThat(smallint.getColumnType(), is(FirebirdBinaryColumnType.SHORT));
        assertThat(smallint.getData(), is(-1234));
        BinaryCell integer = iterator.next();
        assertThat(integer.getColumnType(), is(FirebirdBinaryColumnType.LONG));
        assertThat(integer.getData(), is(123456789));
        BinaryCell nullBigint = iterator.next();
        assertThat(nullBigint.getColumnType(), is(FirebirdBinaryColumnType.INT64));
        assertNull(nullBigint.getData());
        BinaryCell notNumeric = iterator.next();
        assertThat(notNumeric.getColumnType(), is(FirebirdBinaryColumnType.LONG));
        assertThat(notNumeric.getData(), is(7));
        assertFalse(iterator.hasNext());
    }
    
    private FirebirdReturnColumnPacket mockColumn(final FirebirdNumericColumn numericColumn) {
        FirebirdReturnColumnPacket result = mock(FirebirdReturnColumnPacket.class);
        when(result.getNumericColumn()).thenReturn(numericColumn);
        return result;
    }
}
