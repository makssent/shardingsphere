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
import org.firebirdsql.extern.decimal.Decimal;
import org.firebirdsql.extern.decimal.Decimal64;
import org.firebirdsql.extern.decimal.DecimalInconvertibleException;

import java.math.BigDecimal;

/**
 * Binary protocol value for DECFLOAT(16) for Firebird.
 *
 * <p>The value is an IEEE 754 decimal of 8 bytes. Finite values are read as {@link BigDecimal}, NaN and infinities as {@link Decimal64}.</p>
 */
final class FirebirdDecFloat16BinaryProtocolValue implements FirebirdBinaryProtocolValue {
    
    @Override
    public Object read(final FirebirdPacketPayload payload) {
        byte[] bytes = new byte[8];
        payload.getByteBuf().readBytes(bytes);
        Decimal64 result = Decimal64.parseBytes(bytes);
        try {
            return result.toBigDecimal();
        } catch (final DecimalInconvertibleException ignored) {
            return result;
        }
    }
    
    @Override
    public void write(final FirebirdPacketPayload payload, final Object value) {
        payload.getByteBuf().writeBytes(toDecimal(value).toBytes());
    }
    
    private Decimal64 toDecimal(final Object value) {
        if (value instanceof Decimal) {
            return Decimal64.valueOf((Decimal<?>) value);
        }
        if (value instanceof BigDecimal) {
            return Decimal64.valueOf((BigDecimal) value);
        }
        return Decimal64.valueOf(value.toString());
    }
    
    @Override
    public int getLength(final FirebirdPacketPayload payload) {
        return 8;
    }
}
