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

package org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.execute.protocol.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.FirebirdBinaryColumnType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * Numeric scale utility class of Firebird.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class FirebirdNumericScaleUtils {
    
    /**
     * Apply the scale of a message field to its value.
     *
     * <p>SMALLINT, INTEGER, BIGINT and INT128 fields with a scale carry NUMERIC and DECIMAL values as integers in units of {@code 10^scale}.</p>
     *
     * @param type field type
     * @param value value read from the message
     * @param scale field scale from the message BLR
     * @return value with the scale applied
     */
    public static Object applyScale(final FirebirdBinaryColumnType type, final Object value, final int scale) {
        if (0 == scale || !isScaledType(type) || !(value instanceof Number)) {
            return value;
        }
        return new BigDecimal(value instanceof BigInteger ? (BigInteger) value : BigInteger.valueOf(((Number) value).longValue()), -scale);
    }
    
    /**
     * Convert a number to the unscaled integer of a Firebird storage type: the value multiplied by {@code 10^-scale}, rounded half up as Firebird does.
     *
     * @param type storage type
     * @param value value
     * @param scale scale of the column, zero or negative
     * @return {@code Integer} for SHORT and LONG, {@code Long} for INT64, {@code BigInteger} for INT128, the value itself for other types and non-numbers
     * @throws ArithmeticException if the value does not fit the storage type
     */
    public static Object toUnscaledValue(final FirebirdBinaryColumnType type, final Object value, final int scale) {
        if (!isScaledType(type) || !(value instanceof Number)) {
            return value;
        }
        BigInteger unscaled = (value instanceof BigDecimal ? (BigDecimal) value : new BigDecimal(value.toString())).setScale(-scale, RoundingMode.HALF_UP).unscaledValue();
        switch (type) {
            case SHORT:
                return (int) unscaled.shortValueExact();
            case LONG:
                return unscaled.intValueExact();
            case INT64:
                return unscaled.longValueExact();
            default:
                if (unscaled.bitLength() > 127) {
                    throw new ArithmeticException("INT128 overflow: " + unscaled);
                }
                return unscaled;
        }
    }
    
    /**
     * Judge whether field type carries a scale in the message BLR.
     *
     * @param type field type
     * @return whether field type carries a scale
     */
    public static boolean isScaledType(final FirebirdBinaryColumnType type) {
        return FirebirdBinaryColumnType.SHORT == type || FirebirdBinaryColumnType.LONG == type || FirebirdBinaryColumnType.INT64 == type || FirebirdBinaryColumnType.INT128 == type;
    }
}
