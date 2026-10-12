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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.ConversionErrorException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.NumericOutOfRangeException;
import org.apache.shardingsphere.infra.exception.ShardingSpherePreconditions;

/**
 * Text value parser for Firebird.
 *
 * <p>Mirrors how Firebird converts text to a value of another type in {@code CVT_move}: {@code CVT_get_boolean} and {@code CVT_get_double}.</p>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class FirebirdTextValueParser {
    
    private static final int SHORT_LIMIT = (1 << 14) / 5;
    
    private static final int DOUBLE_MAX_10_EXP = 308;
    
    private static final double[] UPPER_POWERS_OF_TEN = {1.e000, 1.e032, 1.e064, 1.e096, 1.e128, 1.e160, 1.e192, 1.e224, 1.e256, 1.e288};
    
    private static final double[] LOWER_POWERS_OF_TEN = {1.e00, 1.e01, 1.e02, 1.e03, 1.e04, 1.e05, 1.e06, 1.e07, 1.e08, 1.e09, 1.e10, 1.e11, 1.e12, 1.e13, 1.e14, 1.e15,
            1.e16, 1.e17, 1.e18, 1.e19, 1.e20, 1.e21, 1.e22, 1.e23, 1.e24, 1.e25, 1.e26, 1.e27, 1.e28, 1.e29, 1.e30, 1.e31};
    
    /**
     * Create conversion error of text as Firebird reports it: bytes below space and above 127 are written as {@code #xhh}.
     *
     * @param text text
     * @return conversion error
     */
    static ConversionErrorException createConversionError(final String text) {
        StringBuilder result = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char each = text.charAt(i);
            result.append(each < ' ' || each > Byte.MAX_VALUE ? String.format("#x%02x", (int) each) : String.valueOf(each));
        }
        return new ConversionErrorException(result.toString());
    }
    
    /**
     * Parse text to boolean.
     *
     * <p>Firebird removes leading and trailing white space and accepts only {@code TRUE} and {@code FALSE} in any case.</p>
     *
     * @param text text
     * @return boolean value
     * @throws ConversionErrorException if the text is not a boolean literal
     */
    public static boolean parseBoolean(final String text) {
        int start = 0;
        int end = text.length();
        while (start < end && isSpace(text.charAt(start))) {
            start++;
        }
        while (end > start && isSpace(text.charAt(end - 1))) {
            end--;
        }
        String value = text.substring(start, end);
        if ("TRUE".equalsIgnoreCase(value)) {
            return true;
        }
        ShardingSpherePreconditions.checkState("FALSE".equalsIgnoreCase(value), () -> createConversionError(text));
        return false;
    }
    
    private static boolean isSpace(final char value) {
        return ' ' == value || value >= '\t' && value <= '\r';
    }
    
    /**
     * Parse text to double.
     *
     * <p>Firebird parses the digits itself: leading and trailing spaces, a sign, one decimal point and an exponent;
     * a power of ten beyond the range of double is out of range.</p>
     *
     * @param text text
     * @return double value
     * @throws ConversionErrorException if the text is not a number
     * @throws NumericOutOfRangeException if the number is out of the range of double
     */
    public static double parseDouble(final String text) {
        double result = 0D;
        int scale = 0;
        int sign = 0;
        boolean digitSeen = false;
        boolean pastSign = false;
        boolean fraction = false;
        int index = skipSpaces(text, 0);
        for (; index < text.length(); index++) {
            char each = text.charAt(index);
            if (isDigit(each)) {
                digitSeen = true;
                pastSign = true;
                if (fraction) {
                    scale++;
                }
                result = result * 10D + (each - '0');
            } else if ('.' == each) {
                ShardingSpherePreconditions.checkState(!fraction, () -> createConversionError(text));
                pastSign = true;
                fraction = true;
            } else if (!pastSign && ('-' == each || '+' == each)) {
                sign = '-' == each ? -1 : 1;
                pastSign = true;
            } else if ('e' == each || 'E' == each) {
                break;
            } else {
                checkSpacesToEnd(text, index);
                index = text.length();
            }
        }
        ShardingSpherePreconditions.checkState(digitSeen, () -> createConversionError(text));
        if (-1 == sign) {
            result = -result;
        }
        if (index < text.length()) {
            scale -= parseExponent(text, index + 1);
        }
        ShardingSpherePreconditions.checkState(Math.abs(scale) <= DOUBLE_MAX_10_EXP, NumericOutOfRangeException::new);
        if (scale > 0) {
            result /= getPowerOfTen(scale);
        } else if (scale < 0) {
            result *= getPowerOfTen(-scale);
        }
        ShardingSpherePreconditions.checkState(!Double.isInfinite(result), NumericOutOfRangeException::new);
        return result;
    }
    
    private static int skipSpaces(final String text, final int start) {
        int result = start;
        while (result < text.length() && ' ' == text.charAt(result)) {
            result++;
        }
        return result;
    }
    
    private static boolean isDigit(final char value) {
        return value >= '0' && value <= '9';
    }
    
    private static void checkSpacesToEnd(final String text, final int start) {
        ShardingSpherePreconditions.checkState(' ' == text.charAt(start) && skipSpaces(text, start) == text.length(), () -> createConversionError(text));
    }
    
    private static int parseExponent(final String text, final int start) {
        int result = 0;
        int sign = 0;
        boolean digitSeen = false;
        for (int index = start; index < text.length(); index++) {
            char each = text.charAt(index);
            if (isDigit(each)) {
                digitSeen = true;
                result = result * 10 + each - '0';
                ShardingSpherePreconditions.checkState(result < SHORT_LIMIT, NumericOutOfRangeException::new);
            } else if (!digitSeen && 0 == sign && ('-' == each || '+' == each)) {
                sign = '-' == each ? -1 : 1;
            } else {
                checkSpacesToEnd(text, index);
                break;
            }
        }
        ShardingSpherePreconditions.checkState(digitSeen, () -> createConversionError(text));
        return -1 == sign ? -result : result;
    }
    
    private static double getPowerOfTen(final int scale) {
        return UPPER_POWERS_OF_TEN[scale >> 5] * LOWER_POWERS_OF_TEN[scale & 0x1F];
    }
    
    /**
     * Parse text to float.
     *
     * <p>Firebird parses the text as double and rejects a value beyond the range of float.</p>
     *
     * @param text text
     * @return float value
     * @throws ConversionErrorException if the text is not a number
     * @throws NumericOutOfRangeException if the number is out of the range of float
     */
    public static float parseFloat(final String text) {
        double result = parseDouble(text);
        ShardingSpherePreconditions.checkState(Math.abs(result) <= Float.MAX_VALUE, NumericOutOfRangeException::new);
        return (float) result;
    }
}
