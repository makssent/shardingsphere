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
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.DateRangeExceededException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.DatetimeRangeExceededException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidTimeZoneOffsetException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidTimeZoneRegionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FirebirdTextDateTimeParserTest {
    
    private static final ZoneId SESSION_ZONE = ZoneId.of("Europe/Moscow");
    
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-12T10:15:30.123456Z"), SESSION_ZONE);
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("dateArguments")
    void assertParseDate(final String name, final String text, final LocalDate expected) {
        assertThat(FirebirdTextDateTimeParser.parseDate(text, CLOCK), is(expected));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidDateArguments")
    void assertParseDateWithInvalidText(final String name, final String text) {
        ConversionErrorException actual = assertThrows(ConversionErrorException.class, () -> FirebirdTextDateTimeParser.parseDate(text, CLOCK));
        assertThat(actual.getValue(), is(text));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("outOfRangeDateArguments")
    void assertParseDateOutOfRange(final String name, final String text) {
        assertThrows(DateRangeExceededException.class, () -> FirebirdTextDateTimeParser.parseDate(text, CLOCK));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("controlCharacterDateArguments")
    void assertParseDateWithControlCharacter(final String name, final String text, final String expectedValue) {
        ConversionErrorException actual = assertThrows(ConversionErrorException.class, () -> FirebirdTextDateTimeParser.parseDate(text, CLOCK));
        assertThat(actual.getValue(), is(expectedValue));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("timeArguments")
    void assertParseTime(final String name, final String text, final LocalTime expected) {
        assertThat(FirebirdTextDateTimeParser.parseTime(text, CLOCK), is(expected));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidTimeArguments")
    void assertParseTimeWithInvalidText(final String name, final String text) {
        ConversionErrorException actual = assertThrows(ConversionErrorException.class, () -> FirebirdTextDateTimeParser.parseTime(text, CLOCK));
        assertThat(actual.getValue(), is(text));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidTimeZoneOffsetArguments")
    void assertParseTimeWithInvalidTimeZoneOffset(final String name, final String text, final String expectedTimeZoneOffset) {
        InvalidTimeZoneOffsetException actual = assertThrows(InvalidTimeZoneOffsetException.class, () -> FirebirdTextDateTimeParser.parseTime(text, CLOCK));
        assertThat(actual.getTimeZoneOffset(), is(expectedTimeZoneOffset));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidTimeZoneRegionArguments")
    void assertParseTimeWithInvalidTimeZoneRegion(final String name, final String text, final String expectedTimeZoneRegion) {
        InvalidTimeZoneRegionException actual = assertThrows(InvalidTimeZoneRegionException.class, () -> FirebirdTextDateTimeParser.parseTime(text, CLOCK));
        assertThat(actual.getTimeZoneRegion(), is(expectedTimeZoneRegion));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("timestampArguments")
    void assertParseTimestamp(final String name, final String text, final LocalDateTime expected) {
        assertThat(FirebirdTextDateTimeParser.parseTimestamp(text, CLOCK), is(expected));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidTimestampArguments")
    void assertParseTimestampWithInvalidText(final String name, final String text) {
        ConversionErrorException actual = assertThrows(ConversionErrorException.class, () -> FirebirdTextDateTimeParser.parseTimestamp(text, CLOCK));
        assertThat(actual.getValue(), is(text));
    }
    
    @Test
    void assertParseTimestampOutOfRange() {
        assertThrows(DatetimeRangeExceededException.class, () -> FirebirdTextDateTimeParser.parseTimestamp("0000-12-31 12:00", CLOCK));
    }
    
    @Test
    void assertParseTimestampWithInvalidTimeZoneBeforeInvalidDate() {
        InvalidTimeZoneOffsetException actual = assertThrows(InvalidTimeZoneOffsetException.class, () -> FirebirdTextDateTimeParser.parseTimestamp("2026-02-30 12:00 +25:00", CLOCK));
        assertThat(actual.getTimeZoneOffset(), is("+25:00"));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("timeWithTimeZoneArguments")
    void assertParseTimeWithTimeZone(final String name, final String text, final ZonedDateTime expected) {
        assertThat(FirebirdTextDateTimeParser.parseTimeWithTimeZone(text, CLOCK), is(expected));
    }
    
    @Test
    void assertParseTimeWithTimeZoneWithDateWord() {
        assertThrows(ConversionErrorException.class, () -> FirebirdTextDateTimeParser.parseTimeWithTimeZone("TODAY", CLOCK));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("timestampWithTimeZoneArguments")
    void assertParseTimestampWithTimeZone(final String name, final String text, final ZonedDateTime expected) {
        assertThat(FirebirdTextDateTimeParser.parseTimestampWithTimeZone(text, CLOCK), is(expected));
    }
    
    @Test
    void assertParseTimestampWithTimeZoneOutOfRange() {
        assertThrows(DatetimeRangeExceededException.class, () -> FirebirdTextDateTimeParser.parseTimestampWithTimeZone("0000-01-01 +03:00", CLOCK));
    }
    
    private static Stream<Arguments> dateArguments() {
        return Stream.of(
                Arguments.of("year_month_day", "2026-10-05", LocalDate.of(2026, 10, 5)),
                Arguments.of("day_month_year_with_point", "05.10.2026", LocalDate.of(2026, 10, 5)),
                Arguments.of("month_day_year_with_slash", "10/05/2026", LocalDate.of(2026, 10, 5)),
                Arguments.of("month_day_year_with_space", "10 05 2026", LocalDate.of(2026, 10, 5)),
                Arguments.of("separator_after_space", "2026 -10 -05", LocalDate.of(2026, 10, 5)),
                Arguments.of("day_month_name_year", "5 OCT 2026", LocalDate.of(2026, 10, 5)),
                Arguments.of("month_name_day_year", "oct 5 2026", LocalDate.of(2026, 10, 5)),
                Arguments.of("year_month_name_day", "2026 October 5", LocalDate.of(2026, 10, 5)),
                Arguments.of("month_name_without_year", "JAN-5", LocalDate.of(2026, 1, 5)),
                Arguments.of("blanks", " \t2026-10-05\t ", LocalDate.of(2026, 10, 5)),
                Arguments.of("two_digit_year_in_this_century", "10-05-75", LocalDate.of(2075, 10, 5)),
                Arguments.of("two_digit_year_in_last_century", "10-05-76", LocalDate.of(1976, 10, 5)),
                Arguments.of("one_digit_fields", "1.2.3", LocalDate.of(2003, 2, 1)),
                Arguments.of("three_digit_year", "100-01-01", LocalDate.of(100, 1, 1)),
                Arguments.of("first_date", "0001-01-01", LocalDate.of(1, 1, 1)),
                Arguments.of("last_date", "9999-12-31", LocalDate.of(9999, 12, 31)),
                Arguments.of("leap_day", "2024-02-29", LocalDate.of(2024, 2, 29)),
                Arguments.of("today", "today", LocalDate.of(2026, 10, 12)),
                Arguments.of("tomorrow", "TOMORROW", LocalDate.of(2026, 10, 13)),
                Arguments.of("yesterday", "Yesterday", LocalDate.of(2026, 10, 11)),
                Arguments.of("now", "NOW", LocalDate.of(2026, 10, 12)),
                Arguments.of("word_with_unchecked_next_character", "TODAY!", LocalDate.of(2026, 10, 12)));
    }
    
    private static Stream<Arguments> invalidDateArguments() {
        return Stream.of(
                Arguments.of("empty", ""),
                Arguments.of("year_only", "2026"),
                Arguments.of("missing_day", "2026-10"),
                Arguments.of("non_existing_day", "2026-02-29"),
                Arguments.of("day_zero", "2026-10-00"),
                Arguments.of("month_zero", "2026-00-05"),
                Arguments.of("month_out_of_range", "26-10-05"),
                Arguments.of("five_digit_year", "12345-01-01"),
                Arguments.of("three_digit_day", "2026-10-005"),
                Arguments.of("time", "2026-10-05 12:00"),
                Arguments.of("trailing_letter", "2026-10-05x"),
                Arguments.of("trailing_punctuation", "2026-10-05."),
                Arguments.of("mixed_separators", "2026/10-05"),
                Arguments.of("space_after_point", "2026.10 05"),
                Arguments.of("short_word", "NO"),
                Arguments.of("unknown_word", "LATER"),
                Arguments.of("long_word", "SEPTEMBERX 1"),
                Arguments.of("word_with_text_after_space", "TODAY !"),
                Arguments.of("word_after_number", "5 TODAY"),
                Arguments.of("two_month_names", "JAN FEB 5"),
                Arguments.of("day_out_of_range_with_month_name", "JANUARY 32 2026"));
    }
    
    private static Stream<Arguments> outOfRangeDateArguments() {
        return Stream.of(Arguments.of("year_zero", "0000-01-01"), Arguments.of("after_last_date", "9999-12-32"));
    }
    
    private static Stream<Arguments> controlCharacterDateArguments() {
        return Stream.of(
                Arguments.of("trailing_control_character", "2026-10-05" + (char) 1, "2026-10-05#x01"),
                Arguments.of("trailing_null", "2026-10-05" + (char) 0, "2026-10-05#x00"),
                Arguments.of("leading_line_feed", (char) 10 + "2026-10-05", "#x0a2026-10-05"));
    }
    
    private static Stream<Arguments> timeArguments() {
        return Stream.of(
                Arguments.of("fraction", "12:34:56.7891", LocalTime.of(12, 34, 56, 789_100_000)),
                Arguments.of("one_digit_fields", "1:2:3", LocalTime.of(1, 2, 3)),
                Arguments.of("hours_and_minutes", "12:34", LocalTime.of(12, 34)),
                Arguments.of("fraction_after_minutes", "12:30.5", LocalTime.of(12, 30, 0, 500_000_000)),
                Arguments.of("blanks", " 12 : 34 ", LocalTime.of(12, 34)),
                Arguments.of("session_time_zone", "12:34 Europe/Moscow", LocalTime.of(12, 34)),
                Arguments.of("time_zone_offset", "12:34 +00:00", LocalTime.of(15, 34)),
                Arguments.of("time_zone_offset_with_blanks", "12:34 + 3 : 05", LocalTime.of(12, 29)),
                Arguments.of("time_zone_offset_without_blank", "12:34-01:00", LocalTime.of(16, 34)),
                Arguments.of("time_zone_region_in_lower_case", "12:34 utc", LocalTime.of(15, 34)),
                Arguments.of("short_time_zone_identifier", "12:00 EST", LocalTime.of(20, 0)),
                Arguments.of("now", "NOW", LocalTime.of(13, 15, 30, 123_000_000)));
    }
    
    private static Stream<Arguments> invalidTimeArguments() {
        return Stream.of(
                Arguments.of("hours_only", "12"),
                Arguments.of("separator_without_minutes", "12:"),
                Arguments.of("five_fraction_digits", "12:34:56.78912"),
                Arguments.of("hours_out_of_range", "25:00"),
                Arguments.of("minutes_out_of_range", "12:60"),
                Arguments.of("three_digit_minutes", "12:034"),
                Arguments.of("fraction_after_hours", "12.5"),
                Arguments.of("time_zone_without_minutes", "12 +03:00"),
                Arguments.of("time_zone_only", "+03:00"),
                Arguments.of("date_word", "TODAY"),
                Arguments.of("word_after_hours", "12:NOW"));
    }
    
    private static Stream<Arguments> invalidTimeZoneOffsetArguments() {
        return Stream.of(
                Arguments.of("minutes_out_of_range", "12:34 +03:60", "+03:60"),
                Arguments.of("after_maximum", "12:34 +14:01", "+14:01"),
                Arguments.of("hours_out_of_range", "12:34 -15:00", "-15:00"),
                Arguments.of("without_minutes", "12:34 +3", "+3"),
                Arguments.of("trailing_text", "12:34 +03:00x", "+03:00x"),
                Arguments.of("without_hours", "12:34 +:30", "+:30"));
    }
    
    private static Stream<Arguments> invalidTimeZoneRegionArguments() {
        return Stream.of(Arguments.of("unknown_region", "12:34 Mars/Base", "Mars/Base"), Arguments.of("trailing_text", "12:34 UTC x", "UTC x"), Arguments.of("leading_digit", "12:34 1UTC", "1UTC"));
    }
    
    private static Stream<Arguments> timestampArguments() {
        return Stream.of(
                Arguments.of("fraction", "2026-10-05 12:00:00.1234", LocalDateTime.of(2026, 10, 5, 12, 0, 0, 123_400_000)),
                Arguments.of("day_month_year_without_seconds", "05.10.2026 12:00", LocalDateTime.of(2026, 10, 5, 12, 0)),
                Arguments.of("date_only", "2026-10-05", LocalDateTime.of(2026, 10, 5, 0, 0)),
                Arguments.of("date_with_time_zone_region", "2026-10-05 Europe/Berlin", LocalDateTime.of(2026, 10, 5, 1, 0)),
                Arguments.of("date_with_time_zone_offset", "2026-10-05 +00:00", LocalDateTime.of(2026, 10, 5, 3, 0)),
                Arguments.of("time_zone_offset", "2026-10-05 12:00 +14:00", LocalDateTime.of(2026, 10, 5, 1, 0)),
                Arguments.of("last_timestamp", "9999-12-31 23:59:59.9999", LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_900_000)),
                Arguments.of("yesterday", "YESTERDAY", LocalDateTime.of(2026, 10, 11, 0, 0)),
                Arguments.of("now", "now", LocalDateTime.of(2026, 10, 12, 13, 15, 30, 123_000_000)));
    }
    
    private static Stream<Arguments> invalidTimestampArguments() {
        return Stream.of(Arguments.of("hours_only", "2026-10-05 12"), Arguments.of("non_existing_day", "2026-02-30 12:00"), Arguments.of("hours_out_of_range", "2026-10-05 24:00"));
    }
    
    private static Stream<Arguments> timeWithTimeZoneArguments() {
        return Stream.of(
                Arguments.of("time_zone_offset", "12:00 +03:00", ZonedDateTime.of(2020, 1, 1, 12, 0, 0, 0, ZoneOffset.ofHours(3))),
                Arguments.of("time_zone_region", "12:00 europe/berlin", ZonedDateTime.of(2020, 1, 1, 12, 0, 0, 0, ZoneId.of("Europe/Berlin"))),
                Arguments.of("session_time_zone", "12:00", ZonedDateTime.of(2020, 1, 1, 12, 0, 0, 0, SESSION_ZONE)),
                Arguments.of("negative_maximum_offset", "12:00 -14:00", ZonedDateTime.of(2020, 1, 1, 12, 0, 0, 0, ZoneOffset.ofHours(-14))),
                Arguments.of("now", "NOW", ZonedDateTime.of(2020, 1, 1, 13, 15, 30, 123_000_000, SESSION_ZONE)));
    }
    
    private static Stream<Arguments> timestampWithTimeZoneArguments() {
        return Stream.of(
                Arguments.of("time_zone_region", "2026-10-05 12:00 Europe/Berlin", ZonedDateTime.of(2026, 10, 5, 12, 0, 0, 0, ZoneId.of("Europe/Berlin"))),
                Arguments.of("session_time_zone", "2026-10-05 12:00", ZonedDateTime.of(2026, 10, 5, 12, 0, 0, 0, SESSION_ZONE)),
                Arguments.of("skipped_wall_time", "2026-03-29 02:30 Europe/Berlin", ZonedDateTime.of(2026, 3, 29, 3, 30, 0, 0, ZoneId.of("Europe/Berlin"))),
                Arguments.of("today_at_utc_midnight", "TODAY", ZonedDateTime.of(2026, 10, 12, 3, 0, 0, 0, SESSION_ZONE)),
                Arguments.of("tomorrow_at_utc_midnight", "TOMORROW", ZonedDateTime.of(2026, 10, 13, 3, 0, 0, 0, SESSION_ZONE)),
                Arguments.of("now", "NOW", ZonedDateTime.of(2026, 10, 12, 13, 15, 30, 123_000_000, SESSION_ZONE)));
    }
}
