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

package org.apache.shardingsphere.database.protocol.firebird.packet.handshake;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.shardingsphere.database.protocol.firebird.constant.FirebirdAuthenticationMethod;
import org.apache.shardingsphere.database.protocol.firebird.payload.FirebirdPacketPayload;
import org.apache.shardingsphere.database.protocol.payload.PacketPayload;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.inOrder;

@ExtendWith(MockitoExtension.class)
class FirebirdAcceptDataPacketTest {
    
    @Mock
    private FirebirdPacketPayload payload;
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("writeArguments")
    void assertWrite(final String name, final byte[] salt, final String publicKey, final FirebirdAuthenticationMethod plugin,
                     final int authenticated, final String keys, final int expectedPayloadLength, final boolean expectedPayloadData) {
        FirebirdAcceptDataPacket packet = new FirebirdAcceptDataPacket(salt, publicKey, plugin, authenticated, keys);
        packet.write((PacketPayload) payload);
        InOrder order = inOrder(payload);
        if (expectedPayloadData) {
            order.verify(payload).writeBuffer(expectedData(salt, publicKey));
        } else {
            order.verify(payload).writeInt4(expectedPayloadLength);
        }
        order.verify(payload).writeString(plugin.getMethodName());
        order.verify(payload).writeInt4(authenticated);
        order.verify(payload).writeString(keys);
        order.verifyNoMoreInteractions();
    }
    
    @Test
    void assertWritePaddedData() {
        ByteBuf buffer = Unpooled.buffer();
        FirebirdAcceptDataPacket packet = new FirebirdAcceptDataPacket(new byte[32], String.join("", Collections.nCopies(254, "A")), FirebirdAuthenticationMethod.SRP256, 0, "");
        packet.write((PacketPayload) new FirebirdPacketPayload(buffer, StandardCharsets.UTF_8));
        assertThat(buffer.getInt(0), is(290));
        assertThat(buffer.getShort(4 + 290), is((short) 0));
        assertThat(buffer.getInt(4 + 292), is(FirebirdAuthenticationMethod.SRP256.getMethodName().length()));
    }
    
    private static byte[] expectedData(final byte[] salt, final String publicKey) {
        byte[] key = publicKey.getBytes(StandardCharsets.US_ASCII);
        byte[] result = new byte[salt.length + key.length + 4];
        result[0] = (byte) salt.length;
        System.arraycopy(salt, 0, result, 2, salt.length);
        result[2 + salt.length] = (byte) key.length;
        System.arraycopy(key, 0, result, 4 + salt.length, key.length);
        return result;
    }
    
    private static Stream<Arguments> writeArguments() {
        return Stream.of(
                Arguments.of("salt_and_public_key", new byte[]{1, 2}, "key", FirebirdAuthenticationMethod.SRP, 1, "k", 9, true),
                Arguments.of("empty_salt", new byte[0], "key", FirebirdAuthenticationMethod.SRP224, 0, "", 0, false),
                Arguments.of("empty_public_key", new byte[]{1, 2}, "", FirebirdAuthenticationMethod.LEGACY_AUTH, 7, "keys", 0, false));
    }
}
