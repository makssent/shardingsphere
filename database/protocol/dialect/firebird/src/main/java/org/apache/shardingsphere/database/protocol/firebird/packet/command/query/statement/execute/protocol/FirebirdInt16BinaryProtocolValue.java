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

import org.apache.shardingsphere.database.protocol.firebird.payload.FirebirdPacketPayload;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Binary protocol value for int16 for Firebird.
 */
public final class FirebirdInt16BinaryProtocolValue implements FirebirdBinaryProtocolValue {
    
    @Override
    public Object read(final FirebirdPacketPayload payload) {
        byte[] result = new byte[16];
        payload.getByteBuf().readBytes(result);
        return new BigInteger(result);
    }
    
    @Override
    public void write(final FirebirdPacketPayload payload, final Object value) {
        byte[] int16 = toBigInteger(value).toByteArray();
        if (int16.length > 16) {
            throw new ArithmeticException("INT128 overflow: " + value);
        }
        int signExtension = int16[0] < 0 ? 0xFF : 0;
        for (int i = int16.length; i < 16; i++) {
            payload.getByteBuf().writeByte(signExtension);
        }
        payload.getByteBuf().writeBytes(int16);
    }
    
    private BigInteger toBigInteger(final Object value) {
        if (value instanceof BigInteger) {
            return (BigInteger) value;
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toBigInteger();
        }
        return BigInteger.valueOf(((Number) value).longValue());
    }
    
    @Override
    public int getLength(final FirebirdPacketPayload payload) {
        return 16;
    }
}
