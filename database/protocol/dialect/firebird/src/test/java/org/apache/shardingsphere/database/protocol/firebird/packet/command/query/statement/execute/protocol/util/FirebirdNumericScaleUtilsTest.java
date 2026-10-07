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
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class FirebirdNumericScaleUtilsTest {
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("assertApplyScaleArguments")
    void assertApplyScale(final String name, final FirebirdBinaryColumnType type, final Object value, final int scale, final Object expected) {
        assertThat(FirebirdNumericScaleUtils.applyScale(type, value, scale), is(expected));
    }
    
    private static Stream<Arguments> assertApplyScaleArguments() {
        return Stream.of(
                Arguments.of("smallint_with_scale", FirebirdBinaryColumnType.SHORT, 9999, -2, new BigDecimal("99.99")),
                Arguments.of("integer_with_negative_value", FirebirdBinaryColumnType.LONG, -123456, -3, new BigDecimal("-123.456")),
                Arguments.of("bigint_with_scale", FirebirdBinaryColumnType.INT64, 12345L, -4, new BigDecimal("1.2345")),
                Arguments.of("bigint_without_scale", FirebirdBinaryColumnType.INT64, 12345L, 0, 12345L),
                Arguments.of("blob_with_scale", FirebirdBinaryColumnType.BLOB, 12345L, -4, 12345L),
                Arguments.of("varying_with_charset", FirebirdBinaryColumnType.VARYING, "foo", 4, "foo"),
                Arguments.of("null_value", FirebirdBinaryColumnType.INT64, null, -4, null));
    }
}
