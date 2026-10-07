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

package org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.FirebirdBinaryColumnType;
import org.apache.shardingsphere.database.protocol.firebird.payload.FirebirdPacketPayload;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class FirebirdBatchMessageCommandPacketTest {
    
    @Test
    void assertGetLengthWithColumnDescriptors() {
        ByteBuf packet = Unpooled.buffer().writeInt(0).writeInt(42).writeInt(1).writeByte(0).writeZero(3).writeInt(100);
        int actual = FirebirdBatchMessageCommandPacket.getLength(new FirebirdPacketPayload(packet, StandardCharsets.UTF_8),
                Collections.singletonList(new FirebirdBatchColumnDescriptor(FirebirdBinaryColumnType.LONG, Integer.BYTES, 0, 0)));
        assertThat(actual, is(20));
    }
    
    @Test
    void assertReadParameterValuesWithScaledColumns() {
        ByteBuf packet = Unpooled.buffer().writeInt(0).writeInt(42).writeInt(1).writeByte(0).writeZero(3).writeInt(-12345).writeLong(12345L);
        FirebirdBatchMessageCommandPacket actual = new FirebirdBatchMessageCommandPacket(new FirebirdPacketPayload(packet, StandardCharsets.UTF_8));
        List<List<Object>> actualValues = actual.readParameterValues(Arrays.asList(
                new FirebirdBatchColumnDescriptor(FirebirdBinaryColumnType.LONG, Integer.BYTES, -2, 0), new FirebirdBatchColumnDescriptor(FirebirdBinaryColumnType.INT64, Long.BYTES, -4, 0)));
        assertThat(actualValues, is(Collections.singletonList(Arrays.asList(new BigDecimal("-123.45"), new BigDecimal("1.2345")))));
    }
}
