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
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.ConversionErrorException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.DateRangeExceededException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.DatetimeRangeExceededException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidTimeZoneOffsetException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidTimeZoneRegionException;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.exception.ShardingSpherePreconditions;
import org.firebirdsql.gds.ng.tz.TimeZoneMapping;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

/**
 * Text date and time parser for Firebird.
 *
 * <p>Mirrors how Firebird converts text to a date or time value in {@code CVT_string_to_datetime}: up to seven numeric components (year, month and day in an order
 * chosen by the text, then hours, minutes, seconds and fractions of a second) separated by white space, date separators and time separators, English month names,
 * the words {@code NOW}, {@code TODAY}, {@code TOMORROW} and {@code YESTERDAY}, and a time zone after the time parsed as {@code TimeZoneUtil::parse} does.
 * The zone of the clock is the session time zone: a value without time zone is converted to it, and a value with time zone without a zone in the text gets it.
 * A time zone region is a name of the backend driver time zone mapping in any case or a short identifier of {@link ZoneId#SHORT_IDS}.</p>
 */
@HighFrequencyInvocation
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
final class FirebirdTextDateTimeParser {
    
    private static final int COMPONENT_COUNT = 7;
    
    private static final int SECOND_DATE_COMPONENT = 1;
    
    private static final int THIRD_DATE_COMPONENT = 2;
    
    private static final int HOUR_COMPONENT = 3;
    
    private static final int MINUTE_COMPONENT = 4;
    
    private static final int SECOND_COMPONENT = 5;
    
    private static final int FRACTION_COMPONENT = 6;
    
    private static final int ENGLISH_MONTH = -1;
    
    private static final int MIN_WORD_LENGTH = 3;
    
    private static final int MAX_WORD_LENGTH = 10;
    
    private static final String[] MONTH_NAMES = {"JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY", "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER"};
    
    private static final String NOW = "NOW";
    
    private static final String TODAY = "TODAY";
    
    private static final String TOMORROW = "TOMORROW";
    
    private static final String YESTERDAY = "YESTERDAY";
    
    private static final int MIN_YEAR_FIRST_DIGITS = 3;
    
    private static final int MAX_YEAR_DIGITS = 4;
    
    private static final int MAX_TWO_DIGIT_YEAR_DIGITS = 2;
    
    private static final int MAX_FIELD_DIGITS = 2;
    
    private static final int FRACTION_DIGITS = 4;
    
    private static final int NANOS_PER_FRACTION = 100_000;
    
    private static final int YEAR_WINDOW = 50;
    
    private static final int MAX_HOURS = 23;
    
    private static final int MAX_MINUTES = 59;
    
    private static final int MAX_SECONDS = 59;
    
    private static final int MAX_OFFSET_HOURS = 14;
    
    private static final int MIN_DATE = -678575;
    
    private static final int MAX_DATE = 2973483;
    
    private static final long BASE_DATE_EPOCH_DAY = LocalDate.of(1858, 11, 17).toEpochDay();
    
    private static final LocalDate TIME_BASE_DATE = LocalDate.of(2020, 1, 1);
    
    private static final Map<String, ZoneId> TIME_ZONE_REGIONS = createTimeZoneRegions();
    
    private final String text;
    
    private final DateTimeType type;
    
    private final Clock clock;
    
    private final int[] components = new int[COMPONENT_COUNT];
    
    private final int[] descriptions = new int[COMPONENT_COUNT];
    
    private int position;
    
    private int index;
    
    private char dateSeparator;
    
    private boolean englishMonth;
    
    private static Map<String, ZoneId> createTimeZoneRegions() {
        Set<String> zoneIds = ZoneId.getAvailableZoneIds();
        Map<String, ZoneId> result = new HashMap<>(zoneIds.size() + ZoneId.SHORT_IDS.size(), 1F);
        TimeZoneMapping timeZoneMapping = TimeZoneMapping.getInstance();
        for (String each : zoneIds) {
            ZoneId zoneId = ZoneId.of(each);
            if (!timeZoneMapping.isOffsetTimeZone(timeZoneMapping.toTimeZoneId(zoneId))) {
                result.put(each.toUpperCase(Locale.ROOT), zoneId);
            }
        }
        for (Entry<String, String> each : ZoneId.SHORT_IDS.entrySet()) {
            result.putIfAbsent(each.getKey(), ZoneId.of(each.getValue()));
        }
        return result;
    }
    
    /**
     * Parse text to date.
     *
     * @param text text
     * @param clock clock in the session time zone
     * @return date
     * @throws ConversionErrorException if the text is not a date
     * @throws DateRangeExceededException if the date is out of the range of dates
     */
    static LocalDate parseDate(final String text, final Clock clock) {
        return new FirebirdTextDateTimeParser(text, DateTimeType.DATE, clock).parseLocalDateTime().toLocalDate();
    }
    
    /**
     * Parse text to time.
     *
     * @param text text
     * @param clock clock in the session time zone
     * @return time in the session time zone
     * @throws ConversionErrorException if the text is not a time
     * @throws InvalidTimeZoneOffsetException if the time zone offset of the text is invalid
     * @throws InvalidTimeZoneRegionException if the time zone region of the text is unknown
     */
    static LocalTime parseTime(final String text, final Clock clock) {
        return new FirebirdTextDateTimeParser(text, DateTimeType.TIME, clock).parseLocalDateTime().toLocalTime();
    }
    
    /**
     * Parse text to timestamp.
     *
     * @param text text
     * @param clock clock in the session time zone
     * @return timestamp in the session time zone
     * @throws ConversionErrorException if the text is not a timestamp
     * @throws DatetimeRangeExceededException if the timestamp is out of the range of timestamps
     * @throws InvalidTimeZoneOffsetException if the time zone offset of the text is invalid
     * @throws InvalidTimeZoneRegionException if the time zone region of the text is unknown
     */
    static LocalDateTime parseTimestamp(final String text, final Clock clock) {
        return new FirebirdTextDateTimeParser(text, DateTimeType.TIMESTAMP, clock).parseLocalDateTime();
    }
    
    /**
     * Parse text to time with time zone.
     *
     * <p>The date of the result is 2020-01-01, the date on which Firebird converts a time with a time zone region.</p>
     *
     * @param text text
     * @param clock clock in the session time zone
     * @return time with time zone
     * @throws ConversionErrorException if the text is not a time
     * @throws InvalidTimeZoneOffsetException if the time zone offset of the text is invalid
     * @throws InvalidTimeZoneRegionException if the time zone region of the text is unknown
     */
    static ZonedDateTime parseTimeWithTimeZone(final String text, final Clock clock) {
        return new FirebirdTextDateTimeParser(text, DateTimeType.TIME_WITH_TIME_ZONE, clock).parseZonedDateTime();
    }
    
    /**
     * Parse text to timestamp with time zone.
     *
     * @param text text
     * @param clock clock in the session time zone
     * @return timestamp with time zone
     * @throws ConversionErrorException if the text is not a timestamp
     * @throws DatetimeRangeExceededException if the timestamp is out of the range of timestamps
     * @throws InvalidTimeZoneOffsetException if the time zone offset of the text is invalid
     * @throws InvalidTimeZoneRegionException if the time zone region of the text is unknown
     */
    static ZonedDateTime parseTimestampWithTimeZone(final String text, final Clock clock) {
        return new FirebirdTextDateTimeParser(text, DateTimeType.TIMESTAMP_WITH_TIME_ZONE, clock).parseZonedDateTime();
    }
    
    private LocalDateTime parseLocalDateTime() {
        Optional<String> specialWord = parseComponents();
        if (specialWord.isPresent()) {
            return getSpecialLocalDateTime(specialWord.get());
        }
        ZoneId zone = parseTimeZone();
        LocalDateTime result = createLocalDateTime();
        return zone.equals(clock.getZone()) ? result : toSessionTimeZone(result, zone);
    }
    
    private LocalDateTime getSpecialLocalDateTime(final String word) {
        LocalDateTime now = LocalDateTime.now(clock).truncatedTo(ChronoUnit.MILLIS);
        return NOW.equals(word) ? now : getSpecialDate(word, now.toLocalDate()).atStartOfDay();
    }
    
    private LocalDateTime toSessionTimeZone(final LocalDateTime localDateTime, final ZoneId zone) {
        if (DateTimeType.TIME == type) {
            LocalTime time = ZonedDateTime.of(LocalDate.now(clock), localDateTime.toLocalTime(), zone).withZoneSameInstant(clock.getZone()).toLocalTime();
            return LocalDateTime.of(localDateTime.toLocalDate(), time);
        }
        return ZonedDateTime.of(localDateTime, zone).withZoneSameInstant(clock.getZone()).toLocalDateTime();
    }
    
    private ZonedDateTime parseZonedDateTime() {
        Optional<String> specialWord = parseComponents();
        if (specialWord.isPresent()) {
            return getSpecialZonedDateTime(specialWord.get());
        }
        ZoneId zone = parseTimeZone();
        return ZonedDateTime.of(createLocalDateTime(), zone);
    }
    
    private ZonedDateTime getSpecialZonedDateTime(final String word) {
        Instant now = clock.instant().truncatedTo(ChronoUnit.MILLIS);
        if (NOW.equals(word)) {
            return DateTimeType.TIME_WITH_TIME_ZONE == type
                    ? ZonedDateTime.of(TIME_BASE_DATE, now.atZone(clock.getZone()).toLocalTime(), clock.getZone())
                    : ZonedDateTime.ofInstant(now, clock.getZone());
        }
        return getSpecialDate(word, now.atOffset(ZoneOffset.UTC).toLocalDate()).atStartOfDay(ZoneOffset.UTC).withZoneSameInstant(clock.getZone());
    }
    
    private Optional<String> parseComponents() {
        index = type.getStartComponent();
        while (index < COMPONENT_COUNT) {
            position = skipBlanks(position);
            if (text.length() == position) {
                break;
            }
            char each = toUpperCase(text.charAt(position));
            if (isLetter(each) && !englishMonth && index - type.getStartComponent() < 2) {
                String word = parseWord();
                int month = findMonth(word);
                if (0 == month) {
                    checkSpecialWordEnd();
                    return Optional.of(word);
                }
                components[index] = month;
                descriptions[index] = ENGLISH_MONTH;
                englishMonth = true;
            } else if (isDigit(each)) {
                parseNumber();
            } else {
                ShardingSpherePreconditions.checkState(DateTimeType.DATE != type && HOUR_COMPONENT == index, this::createConversionError);
                index--;
                break;
            }
            if (!parseSeparator()) {
                break;
            }
            index++;
        }
        ShardingSpherePreconditions.checkState(index - type.getStartComponent() >= 1 && (DateTimeType.DATE != type || index <= THIRD_DATE_COMPONENT), this::createConversionError);
        return Optional.empty();
    }
    
    private String parseWord() {
        int start = position;
        while (position < text.length() && position - start < MAX_WORD_LENGTH && isLetter(toUpperCase(text.charAt(position)))) {
            position++;
        }
        ShardingSpherePreconditions.checkState(position - start >= MIN_WORD_LENGTH, this::createConversionError);
        return text.substring(start, position).toUpperCase(Locale.ROOT);
    }
    
    private int findMonth(final String word) {
        if (index > SECOND_DATE_COMPONENT) {
            return 0;
        }
        for (int i = 0; i < MONTH_NAMES.length; i++) {
            if (MONTH_NAMES[i].startsWith(word)) {
                return i + 1;
            }
        }
        return 0;
    }
    
    private void checkSpecialWordEnd() {
        ShardingSpherePreconditions.checkState(type.getStartComponent() == index, this::createConversionError);
        for (int i = position + 1; i < text.length(); i++) {
            ShardingSpherePreconditions.checkState(isBlank(text.charAt(i)) || '\0' == text.charAt(i), this::createConversionError);
        }
    }
    
    private void parseNumber() {
        int start = position;
        int value = 0;
        while (position < text.length() && isDigit(text.charAt(position))) {
            value = value * 10 + text.charAt(position++) - '0';
        }
        components[index] = value;
        descriptions[index] = position - start;
    }
    
    private boolean parseSeparator() {
        int start = position;
        position = skipBlanks(position);
        if (text.length() == position) {
            return false;
        }
        char separator = text.charAt(position);
        if (index <= SECOND_DATE_COMPONENT) {
            parseDateSeparator(separator, position > start);
            return true;
        }
        if (THIRD_DATE_COMPONENT == index) {
            return true;
        }
        return FRACTION_COMPONENT != index && parseTimeSeparator(separator);
    }
    
    private void parseDateSeparator(final char separator, final boolean hadSpace) {
        boolean separatorAllowed = '\0' == dateSeparator || ' ' != dateSeparator && separator == dateSeparator || ' ' == dateSeparator && hadSpace;
        boolean punctuation = ' ' != dateSeparator && isDateSeparator(separator);
        ShardingSpherePreconditions.checkState(separatorAllowed && (punctuation || hadSpace), this::createConversionError);
        if (punctuation) {
            dateSeparator = separator;
            position++;
        } else {
            dateSeparator = ' ';
        }
    }
    
    private boolean isDateSeparator(final char value) {
        return '/' == value || '-' == value || '.' == value;
    }
    
    private boolean parseTimeSeparator(final char separator) {
        if (':' == separator) {
            position++;
            return true;
        }
        if ('.' == separator) {
            position++;
            index = FRACTION_COMPONENT - 1;
            return true;
        }
        index = COMPONENT_COUNT;
        return false;
    }
    
    private LocalDate getSpecialDate(final String word, final LocalDate today) {
        ShardingSpherePreconditions.checkState(!type.isTime(), this::createConversionError);
        if (TODAY.equals(word)) {
            return today;
        }
        if (TOMORROW.equals(word)) {
            return today.plusDays(1L);
        }
        ShardingSpherePreconditions.checkState(YESTERDAY.equals(word), this::createConversionError);
        return today.minusDays(1L);
    }
    
    private ZoneId parseTimeZone() {
        position = skipBlanks(position);
        if (text.length() == position) {
            return clock.getZone();
        }
        char sign = text.charAt(position);
        return '-' == sign || '+' == sign ? parseTimeZoneOffset(position) : parseTimeZoneRegion(position);
    }
    
    private ZoneOffset parseTimeZoneOffset(final int start) {
        int sign = '-' == text.charAt(start) ? -1 : 1;
        position = skipBlanks(start + 1);
        int hours = parseTimeZoneNumber();
        position = skipBlanks(position);
        ShardingSpherePreconditions.checkState(hours >= 0 && position < text.length() && ':' == text.charAt(position), () -> new InvalidTimeZoneOffsetException(text.substring(start)));
        position = skipBlanks(position + 1);
        int minutes = parseTimeZoneNumber();
        ShardingSpherePreconditions.checkState(minutes >= 0 && text.length() == skipBlanks(position), () -> new InvalidTimeZoneOffsetException(text.substring(start)));
        ShardingSpherePreconditions.checkState(minutes <= MAX_MINUTES && (hours < MAX_OFFSET_HOURS || MAX_OFFSET_HOURS == hours && 0 == minutes),
                () -> new InvalidTimeZoneOffsetException(String.format("%s%02d:%02d", sign < 0 ? "-" : "+", hours, minutes)));
        return ZoneOffset.ofTotalSeconds(sign * (hours * 60 + minutes) * 60);
    }
    
    private int parseTimeZoneNumber() {
        int start = position;
        int result = 0;
        while (position < text.length() && isDigit(text.charAt(position))) {
            result = result * 10 + text.charAt(position++) - '0';
        }
        return start == position ? -1 : result;
    }
    
    private ZoneId parseTimeZoneRegion(final int start) {
        int end = start;
        while (end < text.length() && isTimeZoneRegionCharacter(text.charAt(end), start == end)) {
            end++;
        }
        ZoneId result = text.length() == skipBlanks(end) ? TIME_ZONE_REGIONS.get(text.substring(start, end).toUpperCase(Locale.ROOT)) : null;
        ShardingSpherePreconditions.checkNotNull(result, () -> new InvalidTimeZoneRegionException(text.substring(start)));
        return result;
    }
    
    private boolean isTimeZoneRegionCharacter(final char value, final boolean first) {
        return value >= 'a' && value <= 'z' || isLetter(value) || '_' == value || '/' == value || !first && (isDigit(value) || '+' == value || '-' == value);
    }
    
    private LocalDateTime createLocalDateTime() {
        LocalDate date = type.isTime() ? getTimeBaseDate() : createDate();
        if (index <= THIRD_DATE_COMPONENT) {
            return date.atStartOfDay();
        }
        int fraction = components[FRACTION_COMPONENT];
        for (int i = descriptions[FRACTION_COMPONENT]; i < FRACTION_DIGITS; i++) {
            fraction *= 10;
        }
        return LocalDateTime.of(date, LocalTime.of(components[HOUR_COMPONENT], components[MINUTE_COMPONENT], components[SECOND_COMPONENT], fraction * NANOS_PER_FRACTION));
    }
    
    private LocalDate getTimeBaseDate() {
        checkTime();
        return TIME_BASE_DATE;
    }
    
    private LocalDate createDate() {
        int yearPosition = descriptions[0] >= MIN_YEAR_FIRST_DIGITS ? 0 : THIRD_DATE_COMPONENT;
        int monthPosition = getMonthPosition();
        int dayPosition = THIRD_DATE_COMPONENT + SECOND_DATE_COMPONENT - yearPosition - monthPosition;
        ShardingSpherePreconditions.checkState(descriptions[yearPosition] <= MAX_YEAR_DIGITS && descriptions[monthPosition] <= MAX_FIELD_DIGITS && 0 != descriptions[monthPosition]
                && descriptions[dayPosition] <= MAX_FIELD_DIGITS && descriptions[dayPosition] > 0, this::createConversionError);
        int year = getYear(components[yearPosition], descriptions[yearPosition]);
        int month = components[monthPosition];
        int day = components[dayPosition];
        checkTime();
        int encodedDate = encodeDate(year, month, day);
        ShardingSpherePreconditions.checkState(encodedDate >= MIN_DATE && encodedDate <= MAX_DATE,
                () -> DateTimeType.DATE == type ? new DateRangeExceededException() : new DatetimeRangeExceededException());
        LocalDate result = LocalDate.ofEpochDay(BASE_DATE_EPOCH_DAY + encodedDate);
        ShardingSpherePreconditions.checkState(result.getYear() == year && result.getMonthValue() == month && result.getDayOfMonth() == day, this::createConversionError);
        return result;
    }
    
    private int getMonthPosition() {
        if (descriptions[0] >= MIN_YEAR_FIRST_DIGITS) {
            return SECOND_DATE_COMPONENT;
        }
        if (ENGLISH_MONTH == descriptions[0]) {
            return 0;
        }
        return ENGLISH_MONTH == descriptions[SECOND_DATE_COMPONENT] || '.' == dateSeparator ? SECOND_DATE_COMPONENT : 0;
    }
    
    private int getYear(final int year, final int digits) {
        int currentYear = LocalDate.now(clock).getYear();
        if (0 == digits) {
            return currentYear;
        }
        if (digits > MAX_TWO_DIGIT_YEAR_DIGITS) {
            return year;
        }
        return year < (currentYear - 1900 - YEAR_WINDOW) % 100 ? year + 2000 : year + 1900;
    }
    
    private int encodeDate(final int year, final int month, final int day) {
        int shiftedMonth = month > 2 ? month - 3 : month + 9;
        int shiftedYear = month > 2 ? year : year - 1;
        int century = shiftedYear / 100;
        int yearOfCentury = shiftedYear - 100 * century;
        return (int) (146097L * century / 4 + 1461 * yearOfCentury / 4 + (153 * shiftedMonth + 2) / 5 + day + 1721119 - 2400001);
    }
    
    private void checkTime() {
        ShardingSpherePreconditions.checkState(index <= THIRD_DATE_COMPONENT || isValidTime(), this::createConversionError);
    }
    
    private boolean isValidTime() {
        return components[HOUR_COMPONENT] <= MAX_HOURS && components[MINUTE_COMPONENT] <= MAX_MINUTES && components[SECOND_COMPONENT] <= MAX_SECONDS
                && descriptions[HOUR_COMPONENT] <= MAX_FIELD_DIGITS && 0 != descriptions[HOUR_COMPONENT] && descriptions[MINUTE_COMPONENT] <= MAX_FIELD_DIGITS
                && 0 != descriptions[MINUTE_COMPONENT] && descriptions[SECOND_COMPONENT] <= MAX_FIELD_DIGITS && descriptions[FRACTION_COMPONENT] <= FRACTION_DIGITS;
    }
    
    private int skipBlanks(final int start) {
        int result = start;
        while (result < text.length() && isBlank(text.charAt(result))) {
            result++;
        }
        return result;
    }
    
    private boolean isBlank(final char value) {
        return ' ' == value || '\t' == value;
    }
    
    private char toUpperCase(final char value) {
        return value >= 'a' && value <= 'z' ? (char) (value - 'a' + 'A') : value;
    }
    
    private boolean isLetter(final char value) {
        return value >= 'A' && value <= 'Z';
    }
    
    private boolean isDigit(final char value) {
        return value >= '0' && value <= '9';
    }
    
    private ConversionErrorException createConversionError() {
        return FirebirdTextValueParser.createConversionError(text);
    }
    
    @RequiredArgsConstructor
    @Getter
    private enum DateTimeType {
        
        DATE(0, false),
        
        TIME(HOUR_COMPONENT, true),
        
        TIMESTAMP(0, false),
        
        TIME_WITH_TIME_ZONE(HOUR_COMPONENT, true),
        
        TIMESTAMP_WITH_TIME_ZONE(0, false);
        
        private final int startComponent;
        
        private final boolean time;
    }
}
