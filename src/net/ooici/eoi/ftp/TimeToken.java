/*
 * File Name:  Token.java
 * Created on: May 4, 2011
 */
package net.ooici.eoi.ftp;


import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * TODO Add class comments
 * 
 * @author tlarocque
 * @version 1.0
 */
public class TimeToken implements Comparable<TimeToken> {

    public static final String TOKEN_PATTERN = "%([^%])\\1*?%";

    private final char chr;
    private final int len;
    private final TokenType type;
    private String repr;


    public enum TokenType {
        SECOND('s', Calendar.SECOND),
        MINUTE('m', Calendar.MINUTE),
        HOUR_OF_DAY('H', Calendar.HOUR_OF_DAY),
        DAY_OF_MONTH('d', Calendar.DAY_OF_MONTH),
        DAY_OF_YEAR('D', Calendar.DAY_OF_YEAR),
        MONTH('M', Calendar.MONTH),
        YEAR('y', Calendar.YEAR);

        private final char chr;
        private final int field;

        TokenType(char chr, int calendarField) {
            this.chr = chr;
            this.field = calendarField;
        }

        public char getCharacter() {
            return chr;
        }

        public int getCalendarField() {
            return field;
        }

        public static TokenType getTokenType(char chr) {
            TokenType result = null;

            for (TokenType tp : TokenType.values()) {
                if (tp.chr == chr) {
                    result = tp;
                    break;
                }
            }

            if (null == result)
                throw new IllegalArgumentException("'" + chr + "' does not specify a valid token character");

            return result;
        }
    }


    /**
     * Main ONLY FOR TESTING
     * 
     * @param args
     */
    public static void main(String[] args) {
        String filename = "20110505-MODIS_A-JPL-L2P-A2011125000000.L2_LAC_GHRSST_N-v01.nc.bz2";
        String pattern = "20110505-MODIS_A-JPL-L2P-A%yyyy%%DDD%%HH%%mm%%ss%.L2_LAC_GHRSST_N-v01.nc.bz2";

        String regex = toRegexMatchPattern(pattern);

        System.out.println(regex + "\nMatches? " + (filename.matches(regex) ? "TRUE" : "FALSE"));

    }


    /**
     * Creates a Token object from the given tokenized <code>String</code>
     * 
     * @param s
     *            a String containing a single token in the form "%x%" where x is one or more instances of a token character. For each token
     *            x must be the same character token, and the number of repetitions of that character dictate the presentation of the value
     *            that token represents.
     */
    public TimeToken(String s) {
        if (!s.matches(TOKEN_PATTERN))
            throw new IllegalArgumentException("Invalid token string");

        chr = s.charAt(1);
        len = s.length() - 2;
        type = TokenType.getTokenType(chr);
    }


    /**
     * Creates a new token for the given character, whose presentation length is <code>len</code>
     * 
     * @param chr
     *            The token's character
     * @param len
     *            The token's length
     */
    public TimeToken(char chr, int len) {
        this.chr = chr;
        this.len = len;
        this.type = TokenType.getTokenType(chr);
    }


    public int getLength() {
        return len;
    }

    public char getCharacter() {
        return chr;
    }

    public TokenType getType() {
        return type;
    }


    public static boolean isParsable(String s) {
        int count = 0;
        for (char c : s.toCharArray()) {
            if (c == '%')
                ++count;
        }

        if (count % 2 != 0)
            return false;

        count = count / 2;

        Pattern p = Pattern.compile(TOKEN_PATTERN);
        Matcher m = p.matcher(s);
        int i = 0;
        while (m.find())
            i++;

        return i == count;
    }


    /**
     * Extracts all token substrings from <code>s</code> and creates a list of Token objects from those symbols.
     * 
     * @param s
     *            a String which contains tokens in the form of "%x%". See the constructor of Token for more information about the format of
     *            a token string.
     * 
     * @return a list of Token objects
     * @see {@link #Token(String)}
     */
    public static List<TimeToken> parseTokens(String s) {
        List<TimeToken> result = new ArrayList<TimeToken>();
        Pattern p = Pattern.compile(TOKEN_PATTERN);
        Matcher m = p.matcher(s);

        while (m.find()) {
            TimeToken t = new TimeToken(s.substring(m.start(), m.end()));
            /* allow duplicates -- dont check for contains() */
            result.add(t);
        }

        return result;
    }


    /**
     * Generates a pattern <code>String</code> to be used as the pattern for a DateFormat instance which is representative of the Datasource
     * Pattern given by <code>s</code>.
     * 
     * @param pattern
     * @return
     */
    public static String toDateFormatPattern(String pattern) {

        /* NOTE: This code makes the assumption that the token characters we
         *       are using are synonomous with the pattern characters in the
         *       DateFormat class.
         *       
         *       This assumption's truth can only be maintained by ensuring that
         *       the token character's for each TimeToken are properly mapped to
         *       a Calendar Field in its respective TokenType.
         *       
         */

        /* If the length of s is less than 3 it cannot represent a pattern
         * and we can therefore return s in quoted form as the pattern.
         * 
         * This also prevents IOOB exceptions when the following code would
         * otherwise try to access characters at fixed locations
         */
        if (pattern.length() < 3) {
            return new StringBuilder("'").append(pattern).append("'").toString();
        }


        /* FIXME: This creates a lot of strings; rewrite for memory efficiency */
        pattern = pattern.replace("'", "''");
        if (!pattern.startsWith("%"))
            pattern = "'" + pattern;
        else
            pattern = pattern.substring(1);

        if (!pattern.endsWith("%"))
            pattern = pattern + "'";
        else
            pattern = pattern.substring(0, pattern.length() - 1);

        pattern = pattern.replace("%%", "");
        pattern = pattern.replace("%", "'");




        return pattern;
    }


    /**
     * 
     * @param pattern
     * @return
     */
    public static String toRegexMatchPattern(String pattern) {

        if (!isParsable(pattern)) {
            throw new IllegalArgumentException("Cannot parse the given pattern");
        }

        /* Prefix each symbol in the string with an escape character
         * (but do not prefix the % character)
         */
        // pattern = pattern.replaceAll("([^a-zA-Z0-9\\s%])", "\\\\$1");


        Pattern p = Pattern.compile(TOKEN_PATTERN);
        Matcher m = p.matcher(pattern);


        /* Find each token in the pattern */
        StringBuilder result = new StringBuilder(pattern);
        TimeToken t = null;
        StringBuilder replacement = new StringBuilder();
        int offset = 0;
        while (m.find()) {
            /* Clear the replacement string */
            replacement.delete(0, replacement.length());

            /* Find each token */
            t = new TimeToken(pattern.substring(m.start(), m.end()));

            /* Create a regex replacement for that token */
            replacement.append("([0-9]{");
            replacement.append(t.len);
            replacement.append("})");

            /* Replace the tokenized portion of the string with regex.. */
            result.replace(m.start() + offset, m.end() + offset, replacement.toString());

            /* Adjust the offset so that our index into the resultant string stays
             * in lines up correctly.
             */
            offset += (replacement.length() - t.toString().length());
        }



        return result.toString();
    }

    public static String repeat(String s, int n) {
        StringBuilder sb = new StringBuilder(s.length() * n);

        while (n-- > 0) {
            sb.append(s);
        }

        return sb.toString();
    }


    /**
     * Sorts a list of tokens according to the ordinal of the Token's TokenType. Since TokenTypes are a representation of a time quantity
     * (ex: days, months, years), this ordinal arrangement will sort the tokens from least influential to most influential. That is to say:
     * given two tokens, one representing days and another representing years, the token representing days would be least influential.
     * 
     * @param tokens
     *            an unsorted list of Token objects
     */
    public static void sort(List<TimeToken> tokens) {
        /* Sort the list of tokens according to their ordinal value */
        Collections.sort(tokens, new Comparator<TimeToken>() {

            @Override
            public int compare(TimeToken t1, TimeToken t2) {
                int ord1 = t1.type.ordinal();
                int ord2 = t2.type.ordinal();
                return ord1 < ord2 ? -1 : ord1 > ord2 ? 1 : t1.len < t2.len ? -1 : t1.len > t2.len ? 1 : 0;
            }

        });
    }


    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        if (null == repr) {
            StringBuilder sb = new StringBuilder(len + 2);

            sb.append("%");
            for (int i = 0; i < len; i++) {
                sb.append(chr);
            }
            sb.append("%");

            repr = sb.toString();
        }
        return repr;
    }


    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(TimeToken t) {
        int ord1 = type.ordinal();
        int ord2 = t.type.ordinal();
        return ord1 < ord2 ? -1 : ord1 > ord2 ? 1 : len < t.len ? -1 : len > t.len ? 1 : 0;
    }


    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TimeToken))
            return false;
        return compareTo((TimeToken) obj) == 0;
    }


}
