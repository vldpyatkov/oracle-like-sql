package org.example.ignite.plugin.oraclesql;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jetbrains.annotations.Nullable;

public class SqlFunctions {
    /**
     * Oracle-compatible SUBSTR overload for primitive {@code int} length.
     *
     * @param str Source string.
     * @param pos Start position (1-based, negative values count from the end).
     * @param len Length.
     */
    public static String substr(String str, int pos, int len) {
        return substr(str, pos, Integer.valueOf(len));
    }

    /**
     * Oracle-compatible SUBSTR implementation.
     *
     * @param str Source string.
     * @param pos Start position (1-based, negative values count from the end).
     * @param len Optional length.
     * @return Extracted substring or {@code null}.
     */
    public static String substr(String str, int pos, @Nullable Integer len) {
        if (str == null)
            return null;

        if (len != null && len < 1)
            return null;

        int strLen = str.length();

        int startPos = pos > 0 ? pos - 1 : strLen + pos;

        if (pos == 0)
            startPos = 0;

        if (startPos < 0)
            startPos = 0;

        if (startPos >= strLen)
            return "";

        int endPos = len == null ? strLen : Math.min(strLen, startPos + len);

        if (endPos <= startPos)
            return null;

        return str.substring(startPos, endPos);
    }

    /**
     * Oracle-compatible REGEXP_COUNT implementation with default position and flags.
     */
    public static Integer regexpCount(String str, String pattern) {
        return regexpCount(str, pattern, 1, null);
    }

    /**
     * Oracle-compatible REGEXP_COUNT implementation with default flags.
     */
    public static Integer regexpCount(String str, String pattern, int position) {
        return regexpCount(str, pattern, position, null);
    }

    /**
     * Oracle-compatible REGEXP_COUNT implementation.
     *
     * @param str Source string.
     * @param pattern Regular expression pattern.
     * @param position Start position (1-based).
     * @param matchParam Optional Oracle match parameters (i, c, n, m, x).
     * @return Number of non-overlapping occurrences or {@code null}.
     */
    public static Integer regexpCount(String str, String pattern, int position, @Nullable String matchParam) {
        if (str == null || pattern == null)
            return null;

        if (position < 1)
            throw new IllegalArgumentException("REGEXP_COUNT position must be greater than zero: " + position);

        if (position > str.length())
            return 0;

        Pattern compiled = Pattern.compile(pattern, patternFlags(matchParam));
        Matcher matcher = compiled.matcher(str);

        int idx = position - 1;
        int count = 0;

        while (idx <= str.length() && matcher.find(idx)) {
            count++;

            int nextIdx = matcher.end();

            if (nextIdx == matcher.start())
                nextIdx++;

            idx = nextIdx;
        }

        return count;
    }

    /**
     * Maps Oracle regexp match parameter flags to Java Pattern flags.
     */
    private static int patternFlags(@Nullable String matchParam) {
        int flags = 0;

        if (matchParam == null)
            return flags;

        for (int i = 0; i < matchParam.length(); i++) {
            char ch = Character.toLowerCase(matchParam.charAt(i));

            switch (ch) {
                case 'i':
                    flags |= Pattern.CASE_INSENSITIVE;
                    break;

                case 'c':
                    flags &= ~Pattern.CASE_INSENSITIVE;
                    break;

                case 'n':
                    flags |= Pattern.DOTALL;
                    break;

                case 'm':
                    flags |= Pattern.MULTILINE;
                    break;

                case 'x':
                    flags |= Pattern.COMMENTS;
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported REGEXP_COUNT match parameter: " + ch);
            }
        }

        return flags;
    }

    /**
     * Oracle-compatible SYSTIMESTAMP implementation mapped to Ignite TIMESTAMP.
     *
     * @return Current timestamp.
     */
    public static Timestamp systimestamp() {
        return new Timestamp(System.currentTimeMillis());
    }

    /**
     * Oracle-compatible ADD_MONTHS implementation for DATE.
     *
     * @param date Source date.
     * @param months Number of months to add (can be negative).
     * @return Shifted date or {@code null} when any argument is {@code null}.
     */
    public static Date addMonths(Date date, Integer months) {
        if (date == null || months == null)
            return null;

        LocalDate source = date.toLocalDate();

        return Date.valueOf(addMonthsInternal(source, months));
    }

    /**
     * Oracle-compatible ADD_MONTHS implementation for TIMESTAMP.
     *
     * @param ts Source timestamp.
     * @param months Number of months to add (can be negative).
     * @return Shifted timestamp or {@code null} when any argument is {@code null}.
     */
    public static Timestamp addMonths(Timestamp ts, Integer months) {
        if (ts == null || months == null)
            return null;

        LocalDateTime source = ts.toLocalDateTime();
        LocalDate shiftedDate = addMonthsInternal(source.toLocalDate(), months);

        return Timestamp.valueOf(LocalDateTime.of(shiftedDate, source.toLocalTime()));
    }

    /**
     * Oracle-compatible ADD_MONTHS implementation for primitive month argument.
     */
    public static Date addMonths(Date date, int months) {
        return addMonths(date, Integer.valueOf(months));
    }

    /**
     * Oracle-compatible ADD_MONTHS implementation for primitive month argument.
     */
    public static Timestamp addMonths(Timestamp ts, int months) {
        return addMonths(ts, Integer.valueOf(months));
    }

    /**
     * Applies Oracle month-shift rules:
     * if source date is month end, result is target month end;
     * if target month is shorter than source day, result is target month end.
     */
    private static LocalDate addMonthsInternal(LocalDate source, int months) {
        LocalDate target = source.plusMonths(months);

        if (source.getDayOfMonth() == source.lengthOfMonth())
            return target.withDayOfMonth(target.lengthOfMonth());

        if (source.getDayOfMonth() > target.lengthOfMonth())
            return target.withDayOfMonth(target.lengthOfMonth());

        return target.withDayOfMonth(source.getDayOfMonth());
    }
}
