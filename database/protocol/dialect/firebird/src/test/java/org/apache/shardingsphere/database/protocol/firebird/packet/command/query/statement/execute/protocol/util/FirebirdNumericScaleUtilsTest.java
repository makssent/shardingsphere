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

import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.FirebirdBinaryColumnType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FirebirdNumericScaleUtilsTest {
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("assertApplyScaleArguments")
    void assertApplyScale(final String name, final FirebirdBinaryColumnType type, final Object value, final int scale, final Object expected) {
        assertThat(FirebirdNumericScaleUtils.applyScale(type, value, scale), is(expected));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("assertToUnscaledValueArguments")
    void assertToUnscaledValue(final String name, final FirebirdBinaryColumnType type, final Object value, final int scale, final Object expected) {
        assertThat(FirebirdNumericScaleUtils.toUnscaledValue(type, value, scale), is(expected));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("assertToUnscaledValueOverflowArguments")
    void assertToUnscaledValueOverflow(final String name, final FirebirdBinaryColumnType type, final Object value, final int scale) {
        assertThrows(ArithmeticException.class, () -> FirebirdNumericScaleUtils.toUnscaledValue(type, value, scale));
    }
    
    private static Stream<Arguments> assertApplyScaleArguments() {
        return Stream.of(
                Arguments.of("smallint_with_scale", FirebirdBinaryColumnType.SHORT, 9999, -2, new BigDecimal("99.99")),
                Arguments.of("integer_with_negative_value", FirebirdBinaryColumnType.LONG, -123456, -3, new BigDecimal("-123.456")),
                Arguments.of("bigint_with_scale", FirebirdBinaryColumnType.INT64, 12345L, -4, new BigDecimal("1.2345")),
                Arguments.of("bigint_without_scale", FirebirdBinaryColumnType.INT64, 12345L, 0, 12345L),
                Arguments.of("int128_with_scale", FirebirdBinaryColumnType.INT128, new BigInteger("-123456789012345678901234567890"), -4, new BigDecimal("-12345678901234567890123456.7890")),
                Arguments.of("int128_without_scale", FirebirdBinaryColumnType.INT128, new BigInteger("17014118346046923173168730371588410572"), 0,
                        new BigInteger("17014118346046923173168730371588410572")),
                Arguments.of("blob_with_scale", FirebirdBinaryColumnType.BLOB, 12345L, -4, 12345L),
                Arguments.of("varying_with_charset", FirebirdBinaryColumnType.VARYING, "foo", 4, "foo"),
                Arguments.of("null_value", FirebirdBinaryColumnType.INT64, null, -4, null));
    }
    
    private static Stream<Arguments> assertToUnscaledValueArguments() {
        return Stream.of(
                Arguments.of("smallint", FirebirdBinaryColumnType.SHORT, new BigDecimal("-12.34"), -2, -1234),
                Arguments.of("integer", FirebirdBinaryColumnType.LONG, new BigDecimal("123456.789"), -3, 123456789),
                Arguments.of("integer_rounded_half_up", FirebirdBinaryColumnType.LONG, new BigDecimal("1.235"), -2, 124),
                Arguments.of("negative_rounded_half_up", FirebirdBinaryColumnType.LONG, new BigDecimal("-1.235"), -2, -124),
                Arguments.of("integer_without_scale", FirebirdBinaryColumnType.LONG, 7, 0, 7),
                Arguments.of("bigint", FirebirdBinaryColumnType.INT64, new BigDecimal("-99999999999999.9999"), -4, -999999999999999999L),
                Arguments.of("int128", FirebirdBinaryColumnType.INT128, new BigDecimal("12345678901234567890123456.7890"), -4, new BigInteger("123456789012345678901234567890")),
                Arguments.of("decfloat", FirebirdBinaryColumnType.DEC16, new BigDecimal("1.5"), 0, new BigDecimal("1.5")),
                Arguments.of("null_value", FirebirdBinaryColumnType.INT64, null, -4, null));
    }
    
    private static Stream<Arguments> assertToUnscaledValueOverflowArguments() {
        return Stream.of(
                Arguments.of("smallint", FirebirdBinaryColumnType.SHORT, new BigDecimal("327.68"), -2),
                Arguments.of("int128", FirebirdBinaryColumnType.INT128, new BigDecimal(BigInteger.ONE.shiftLeft(127)), 0));
    }
}
