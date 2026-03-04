package org.example.ignite.plugin.oraclesql;

import java.time.OffsetDateTime;
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
     * Oracle-compatible SYSTIMESTAMP implementation.
     *
     * @return Current timestamp with local time zone offset.
     */
    public static OffsetDateTime systimestamp() {
        return OffsetDateTime.now();
    }
}
