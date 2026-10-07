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

package org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.execute.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.shardingsphere.database.protocol.firebird.payload.FirebirdPacketPayload;
import org.firebirdsql.extern.decimal.Decimal64;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class FirebirdDecFloat16BinaryProtocolValueTest {
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("readArguments")
    void assertRead(final String name, final Decimal64 value, final Object expected) {
        FirebirdPacketPayload payload = new FirebirdPacketPayload(Unpooled.wrappedBuffer(value.toBytes()), StandardCharsets.UTF_8);
        assertThat(new FirebirdDecFloat16BinaryProtocolValue().read(payload), is(expected));
        assertThat(payload.getByteBuf().readableBytes(), is(0));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("writeArguments")
    void assertWrite(final String name, final Object value, final Decimal64 expected) {
        ByteBuf buffer = Unpooled.buffer();
        new FirebirdDecFloat16BinaryProtocolValue().write(new FirebirdPacketPayload(buffer, StandardCharsets.UTF_8), value);
        byte[] actual = new byte[buffer.readableBytes()];
        buffer.readBytes(actual);
        assertThat(actual, is(expected.toBytes()));
    }
    
    @Test
    void assertGetLength() {
        assertThat(new FirebirdDecFloat16BinaryProtocolValue().getLength(new FirebirdPacketPayload(Unpooled.buffer(), StandardCharsets.UTF_8)), is(8));
    }
    
    private static Stream<Arguments> readArguments() {
        return Stream.of(
                Arguments.of("finite", Decimal64.valueOf("-12345.6789"), new BigDecimal("-12345.6789")),
                Arguments.of("nan", Decimal64.POSITIVE_NAN, Decimal64.POSITIVE_NAN),
                Arguments.of("negative_infinity", Decimal64.NEGATIVE_INFINITY, Decimal64.NEGATIVE_INFINITY));
    }
    
    private static Stream<Arguments> writeArguments() {
        return Stream.of(
                Arguments.of("big_decimal", new BigDecimal("1.5"), Decimal64.valueOf("1.5")),
                Arguments.of("decimal", Decimal64.POSITIVE_INFINITY, Decimal64.POSITIVE_INFINITY),
                Arguments.of("integer", 7, Decimal64.valueOf("7")));
    }
}
