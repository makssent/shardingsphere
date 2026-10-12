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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob;

import org.apache.shardingsphere.database.exception.firebird.exception.protocol.ConversionErrorException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.NumericOutOfRangeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FirebirdTextValueParserTest {
    
    @Test
    void assertParseBooleanWithTrue() {
        assertTrue(FirebirdTextValueParser.parseBoolean(" \ttrue\r\n"));
    }
    
    @Test
    void assertParseBooleanWithFalse() {
        assertFalse(FirebirdTextValueParser.parseBoolean("FaLsE"));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidBooleanArguments")
    void assertParseBooleanWithInvalidText(final String name, final String text, final String expectedValue) {
        ConversionErrorException actual = assertThrows(ConversionErrorException.class, () -> FirebirdTextValueParser.parseBoolean(text));
        assertThat(actual.getValue(), is(expectedValue));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("doubleArguments")
    void assertParseDouble(final String name, final String text, final double expected) {
        assertThat(FirebirdTextValueParser.parseDouble(text), is(expected));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidDoubleArguments")
    void assertParseDoubleWithInvalidText(final String name, final String text) {
        assertThrows(ConversionErrorException.class, () -> FirebirdTextValueParser.parseDouble(text));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("outOfRangeDoubleArguments")
    void assertParseDoubleOutOfRange(final String name, final String text) {
        assertThrows(NumericOutOfRangeException.class, () -> FirebirdTextValueParser.parseDouble(text));
    }
    
    @Test
    void assertParseFloat() {
        assertThat(FirebirdTextValueParser.parseFloat("1.5"), is(1.5F));
    }
    
    @Test
    void assertParseFloatOutOfRange() {
        assertThrows(NumericOutOfRangeException.class, () -> FirebirdTextValueParser.parseFloat("3.5e38"));
    }
    
    private static Stream<Arguments> invalidBooleanArguments() {
        return Stream.of(
                Arguments.of("yes", "yes", "yes"),
                Arguments.of("one", "1", "1"),
                Arguments.of("empty", "", ""),
                Arguments.of("control_character", "T" + (char) 1, "T#x01"));
    }
    
    private static Stream<Arguments> doubleArguments() {
        return Stream.of(
                Arguments.of("spaces", "  1.5  ", 1.5D),
                Arguments.of("sign", "-2", -2D),
                Arguments.of("exponent", "1.5e3", 1500D),
                Arguments.of("negative_exponent", "+25E-1", 2.5D),
                Arguments.of("leading_point", ".5", 0.5D),
                Arguments.of("exponent_with_spaces", "1e2  ", 100D));
    }
    
    private static Stream<Arguments> invalidDoubleArguments() {
        return Stream.of(
                Arguments.of("no_digits", "   "),
                Arguments.of("two_points", "1.2.3"),
                Arguments.of("tab", "1\t"),
                Arguments.of("space_inside", "1 2"),
                Arguments.of("infinity", "inf"),
                Arguments.of("exponent_without_digits", "1e"),
                Arguments.of("sign_after_digit", "1-"));
    }
    
    private static Stream<Arguments> outOfRangeDoubleArguments() {
        return Stream.of(
                Arguments.of("too_large", "1e309"),
                Arguments.of("too_small", "1e-309"),
                Arguments.of("long_exponent", "1e3276"),
                Arguments.of("infinite", "9e308"));
    }
}
