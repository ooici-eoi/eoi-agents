/*
 * File Name:  TestFtp.java
 * Created on: May 3, 2011
 */
package net.ooici.eoi.ftp;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;


/**
 * TODO Add class comments
 * 
 * @author tlarocque
 * @version 1.0
 */
public class FtpFileFinder {

    /** TODO: Rewrite getTargetDirs() so that it orders all tokens by their token properties,
     *        then remove generateTimeRange() and replace with code that can produce the replacements
     *        list by 1) performing Calendar.add([lowest token props].getCalendarField, 1) and then 2) retrieves
     *        the date.  Next 3) it uses each tokens Token.getTokenProperties.getCalendarField to retrieve the
     *        value it should be replaced with.
     */
    
    
    public static void main(String[] args) throws IOException {
        EasyFtp ftp = null;
        
        
        /* Testing FTP access */
//        ftp = new EasyFtp("podaac.jpl.nasa.gov");
//        ftp.cd("/allData/ghrsst/data/L2P/MODIS_A/JPL/");
//        System.out.println(ftp.list("2011/001/", ".*A[0-9]+?0000.*\\.bz2$"));
//        ftp.close();
        
        
        /* Testing TOKENS */
//        String pattern = "%yyyy%/%DDD%/%yy%/%DD%/%dd%/%yyyy%";
//        List<Token> tokens = Token.getTokens(pattern);
//        for (Token t : tokens) {
//            System.out.println(t);
//        }
//        
//        System.out.println("************************");
//        
//        Token.sort(tokens);
//        for (Token t : tokens) {
//            System.out.println(t);
//        }
        
        /* Testings GenerateDateFormatPattern() */
//        testGenerateDateFormatPattern();
//        System.exit(0);
        
        /* Testing isParsablePattern */
        String p = "%yyy%sss%ddd"; 
        System.out.println(TimeToken.isParsablePattern(p));
        System.exit(0);
        
        
        
        /* Testing getTargetDirs */
        String pattern = "%yyyy%/%DDD%/";
        
        if (! TimeToken.isParsablePattern(pattern)) {
            System.out.println("Pattern is invalid!");
            System.exit(1);
        }
        
    
        Calendar startCal = createUtcCal();
        Calendar endCal = createUtcCal();
        
        startCal.set(2011, Calendar.FEBRUARY, 15);
        endCal.set(2011, Calendar.MARCH, 10);
        
        long start = startCal.getTimeInMillis();
        long end = endCal.getTimeInMillis();
        
        List<String> dirs = getTargetDirs(pattern, start, end);
        for (String dir : dirs) {
            System.out.println(">>>>  " + dir);
        }
        
    }
    
    
    public static void testGenerateDateFormatPattern() {
        
        String[] patterns = new String[20];
        int i = 0;
        patterns[i++] = "Ab";
        patterns[i++] = "%yyyy%XX";
        patterns[i++] = "XX%yyyy%";
        patterns[i++] = "XX%yyyy%XX";
        patterns[i++] = "%yyyy%XX%yyyy%";
        patterns[i++] = "%yyyy%X'X";
        patterns[i++] = "X'X%yyyy%";
        patterns[i++] = "X'X%yyyy%X'X";
        patterns[i++] = "%yyyy%X'X%yyyy%";
        patterns[i++] = "'XX%yyyy%";
        patterns[i++] = "%yyyy%XX'";
        patterns[i++] = "%yyyy%%dd%%DD%";

        String[] answers = new String[20];
        int j = 0;
        answers[j++] = "Ab";
        answers[j++] = "2011XX";
        answers[j++] = "XX2011";
        answers[j++] = "XX2011XX";
        answers[j++] = "2011XX2011";
        answers[j++] = "2011X'X";
        answers[j++] = "X'X2011";
        answers[j++] = "X'X2011X'X";
        answers[j++] = "2011X'X2011";
        answers[j++] = "'XX2011";
        answers[j++] = "2011XX'";
        answers[j++] = "20110101";
        
        DateFormat df = null;
        Calendar cal = new GregorianCalendar(2011, 00, 01);
        String result = "";
        for (int k = 0; k < i; k++) {
            patterns[k] = TimeToken.generateDateFormatPattern(patterns[k]);
            System.out.println("\n--------------------------\n" + patterns[k]);
            
            df = new SimpleDateFormat(patterns[k]);
            result = df.format(cal.getTime());
            
            assert result.equals(answers[k]) : result + " != " + answers[k];
            System.out.println(result + " = " + answers[k]);
        }
        
    }
                                                 
    
    public static Calendar createUtcCal() {
        return createUtcCal(0);
    }
    
    public static Calendar createUtcCal(long millis) {
        Calendar result = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        result.setTimeInMillis(millis);
        return result;
    }
    
    public static List<String> getTargetDirs(String pattern, long startTime, long endTime) {
        List<String> results = new ArrayList<String>();
        
        /* Get a list of all tokens in the pattern string */
        List<TimeToken> tokens = TimeToken.parseTokens(pattern);
        for (TimeToken t : tokens) {
            System.out.println("Character: " + t.getCharacter() + "\t\tCount: " + t.getLength());
        }
        
        /* If there are no tokens, simply return pattern as our target directory */
        if (tokens.size() == 0) {
            results.add(pattern);
            return results;
        }
        
        /* Create a DateFormat pattern with the given tokenized pattern */
        String dfPattern = TimeToken.generateDateFormatPattern(pattern);
        
        
        /* Sort the list of tokens according to their ordinal value */
        TimeToken.sort(tokens);
        
        /* Uses the lowest ordinal value as an incrementor to generate dates between start date and end date,
         * This is possible because tokens are mapped to a calendar field via their TokenType */
        int incField = tokens.get(0).getType().getCalendarField();
        
        /* Iterate over the dates between startTime and endTime and use these to generate directory strings */
        Calendar startCal = createUtcCal(startTime);
        Calendar endCal = createUtcCal(endTime);
        DateFormat sdf = new SimpleDateFormat(dfPattern);
        String result = "";
        do {
            result = sdf.format(startCal.getTime());
            if (! results.contains(result)) {
                results.add(result);
            }
            
            startCal.add(incField, 1);
        } while (startCal.compareTo(endCal) < 0);
        
        
        
        return results;
    }
    
    
}
