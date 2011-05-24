/*
 * File Name:  TestFtp.java
 * Created on: May 3, 2011
 */
package net.ooici.eoi.ftp;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * TODO Add class comments
 * 
 * @author tlarocque
 * @version 1.0
 */
public class FtpFileFinder {

    /** Static Fields */
    static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FtpFileFinder.class);
    private static final String NEW_LINE = System.getProperty("line.separator");
    private static final String TAB_STR = "    ";


    public static void main(String[] args) throws IOException {


        /* Testing FTP access */
        // ftp = new EasyFtp("podaac.jpl.nasa.gov");
        // ftp.cd("/allData/ghrsst/data/L2P/MODIS_A/JPL/");
        // log.debug(ftp.list("2011/001/", ".*A[0-9]+?0000.*\\.bz2$"));
        // ftp.close();


        /* Testing TOKENS */
        // String pattern = "%yyyy%/%DDD%/%yy%/%DD%/%dd%/%yyyy%";
        // List<Token> tokens = Token.getTokens(pattern);
        // for (Token t : tokens) {
        // log.debug(t);
        // }
        //
        // log.debug("************************");
        //
        // Token.sort(tokens);
        // for (Token t : tokens) {
        // log.debug(t);
        // }

        /* Testings GenerateDateFormatPattern() */
        // testGenerateDateFormatPattern();
        // System.exit(0);

        /* Testing isParsablePattern */
        // String p = "%yyy%sss%ddd";
        // log.debug(TimeToken.isParsable(p));
        // System.exit(0);



        /* Testing getTargetDirs */
        // String pattern = "%yyyy%/%MM%/(%DDD%|%dd%)%HH%.%mm%/";
        //
        // if (! TimeToken.isParsable(pattern)) {
        // log.debug("Pattern is invalid!");
        // }
        // log.debug("\n\n*********************************\n\n");
        //
        //
        // Calendar startCal = createUtcCal();
        // Calendar endCal = createUtcCal();
        //
        // startCal.set(2011, Calendar.MARCH, 5);
        // endCal.set(2011, Calendar.MARCH, 10);
        //
        // long start = startCal.getTimeInMillis();
        // long end = endCal.getTimeInMillis();
        //
        // long debugStartTime = System.currentTimeMillis();
        // List<String> dirs = getTargetDirs(pattern, start, end);
        // log.debug("End Time = " + ((System.currentTimeMillis() - debugStartTime) / 1000.0));
        // for (String dir : dirs) {
        // log.debug(">>>>  " + dir);
        // }


        // testGetTargetFiles();

         testGenerateNcml_hycom();

//         testGenerateNcml_ostia();

//        testUrlParser();
    }


    private static void testUrlParser() {
        // String s = "ftp://www.google.com/stuff/here.nc";
        String s = "http://www.google.com";


        UrlParser p = new UrlParser(s);

        System.out.println('"' + p.fullUrl + '"');
        System.out.println('"' + p.protocol + '"');
        System.out.println('"' + p.host + '"');
        System.out.println('"' + p.directory + '"');
        System.out.println('"' + p.file + '"');
    }


    private static void testGenerateNcml_hycom() throws IOException {

        Map<String, Long> filemap = new TreeMap<String, Long>();
        filemap.put("/Users/tlarocque/Downloads/ooici/hycom/909_archv.2011050818_2011050700_idp_EastCst1.nc", 7L);
        filemap.put("/Users/tlarocque/Downloads/ooici/hycom/909_archv.2011050818_2011050700_sal_EastCst1.nc", 7L);
        filemap.put("/Users/tlarocque/Downloads/ooici/hycom/909_archv.2011050818_2011050700_ssh_EastCst1.nc", 7L);
        filemap.put("/Users/tlarocque/Downloads/ooici/hycom/909_archv.2011050818_2011050700_tem_EastCst1.nc", 7L);
        filemap.put("/Users/tlarocque/Downloads/ooici/hycom/909_archv.2011050818_2011050700_uvl_EastCst1.nc", 7L);
        filemap.put("/Users/tlarocque/Downloads/ooici/hycom/909_archv.2011050818_2011050700_vvl_EastCst1.nc", 7L);
        filemap.put("/Users/tlarocque/Downloads/ooici/hycom/909_archv.2011050818_2011050800_idp_EastCst1.nc", 8L);
        filemap.put("/Users/tlarocque/Downloads/ooici/hycom/909_archv.2011050818_2011050800_sal_EastCst1.nc", 8L);
        filemap.put("/Users/tlarocque/Downloads/ooici/hycom/909_archv.2011050818_2011050800_ssh_EastCst1.nc", 8L);
        filemap.put("/Users/tlarocque/Downloads/ooici/hycom/909_archv.2011050818_2011050800_tem_EastCst1.nc", 8L);
        filemap.put("/Users/tlarocque/Downloads/ooici/hycom/909_archv.2011050818_2011050800_uvl_EastCst1.nc", 8L);
        filemap.put("/Users/tlarocque/Downloads/ooici/hycom/909_archv.2011050818_2011050800_vvl_EastCst1.nc", 8L);



        log.debug("Generating union/join NCML file...");
        File temp = File.createTempFile("ooi-", ".ncml");
        // temp.deleteOnExit();
        String ncml = "<netcdf xmlns=\"http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2\">\n      <variable name=\"lat\">\n         <attribute name=\"moto\" type=\"string\" value=\"GO TEAM!\" />\n      </variable>\n      <variable name=\"lon\">\n         <attribute name=\"moto\" type=\"string\" value=\"GO TEAM!\" />\n      </variable>\n</netcdf>\n";
        
        generateNcml(temp, filemap, "MT", ncml);
        log.debug("... COMPLETE!");
        log.debug("");
        log.debug("");
        log.debug("Reference: " + temp.getAbsolutePath());
    }


    private static void testGenerateNcml_ostia() throws IOException {

        Map<String, Long> filemap = new TreeMap<String, Long>();
        filemap.put("/Users/tlarocque/Downloads/20110102-MODIS_A-JPL-L2P-A2011002134000.L2_LAC_GHRSST_N-v01.nc", 1L);
        filemap.put("/Users/tlarocque/Downloads/20110102-MODIS_A-JPL-L2P-A2011002134000.L2_LAC_GHRSST_D-v01.nc", 1L);



        log.debug("Generating union/join NCML file...");
        File temp = File.createTempFile("ooi-", ".ncml");
        // temp.deleteOnExit();
        generateNcml(temp, filemap, "time");
        log.debug("... COMPLETE!");
        log.debug("");
        log.debug("");
        log.debug("Reference: " + temp.getAbsolutePath());
    }


    private static void testGetTargetFiles() throws IOException {
        /* Test getTargetFiles */

        /** MODIS_A */
        // ftp://podaac.jpl.nasa.gov/allData/ghrsst/data/L2P/MODIS_A/JPL/
        // String host = "podaac.jpl.nasa.gov";
        // String baseDir = "allData/ghrsst/data/L2P/MODIS_A/JPL/";
        //
        // String filePattern = "%yyyy%%MM%%dd%-MODIS_A-JPL-L2P-A%yyyy%%DDD%%HH%%mm%%ss%\\.L2_LAC_GHRSST_[a-zA-Z]-v01\\.nc\\.bz2";
        // String dirPattern = "%yyyy%/%DDD%/";
        //
        // Calendar startCal = createUtcCal(0);
        // Calendar endCal = createUtcCal(0);
        // startCal.set(2011, Calendar.JANUARY, 2);
        // startCal.set(Calendar.HOUR_OF_DAY, 13);
        // startCal.set(Calendar.MINUTE, 50);
        // endCal.set(2011, Calendar.JANUARY, 4);
        // endCal.set(Calendar.HOUR_OF_DAY, 13);
        // endCal.set(Calendar.MINUTE, 50);
        // long startTime = startCal.getTimeInMillis();
        // long endTime = endCal.getTimeInMillis();

        /** GLIDERS */
        /**
         * filePattern is NOT correct -- this is for testing only, %DDD% should actually be the 4-digit glider number
         */
        String host = "ftp.soest.hawaii.edu";
        String baseDir = "pilot/sg500/";

        String filePattern = "p500[0-9]%DDD%\\.nc\\.gz";
        String dirPattern = "";

        Calendar startCal = createUtcCal(0);
        Calendar endCal = createUtcCal(0);
        startCal.set(1970, Calendar.JANUARY, 25);
        endCal.set(1970, Calendar.JANUARY, 38);
        long startTime = startCal.getTimeInMillis();
        long endTime = endCal.getTimeInMillis();



        Map<String, Long> remoteFiles = getTargetFiles(host, baseDir, filePattern, dirPattern, startTime, endTime);


        log.debug("\n\n\n*************************************************************\n\n\n");
        // int minutes_ctr = 13 * 60 + 50;
        // int day_in_minutes = 24 * 60;
        // Format fmt = new DecimalFormat("#00");

        // Pattern p = Pattern.compile("([0-9]{4})(?=00\\.L2)");
        // Matcher m = null;
        // int totalError = 0;
        for (String file : remoteFiles.keySet()) {
            log.debug(file);
            //
            // m = p.matcher(file);
            // m.find();
            // StringBuilder compare = new StringBuilder();
            // int minutes = minutes_ctr % 60;
            // int hours = minutes_ctr / 60 % 24;
            // compare.append(fmt.format(hours));
            // compare.append(fmt.format(minutes));
            //
            // if (! compare.toString().equals(m.group())) {
            // try {
            // minutes_ctr -= 5;
            // totalError++;
            // throw new AssertionError("MISSING FILE!");
            // } catch (AssertionError e) {
            // e.printStackTrace();
            // }
            // }
            //
            // minutes_ctr += 5;
            // if (minutes_ctr >= day_in_minutes)
            // minutes_ctr = 0;
        }
        log.debug("\n\n\n*************************************************************\n\n\n");
        log.debug("Total files: " + remoteFiles.size());
        // log.debug("Total extras: " + totalError);

        log.debug("\n\nDOWNLOADING...");
        EasyFtp ftp = new EasyFtp(host);
        ftp.cd(baseDir);
        Map<String, Long> localFiles = new TreeMap<String, Long>();

        for (String key : remoteFiles.keySet()) {
            /* Download the file */
            String download = ftp.download(key, "/Users/tlarocque/Downloads/ooici/", !log.isDebugEnabled());
            log.debug("\n\n" + download);


            /* Test unzipping... */
            String unzipped = EasyFtp.unzip(download, !log.isDebugEnabled()).get(0);
            log.debug(unzipped);


            /* Insert the new output name back into the map */
            Long val = remoteFiles.get(key);
            localFiles.put(unzipped, val);
        }

        /* Try generating an NCML (time aggregation) */
        String outputNcml = "/Users/tlarocque/Downloads/ooici/ncml/glider_temp.ncml";
        generateNcml(outputNcml, localFiles, "time");

        log.debug("\n\nGenerated NCML aggregation...\n\t\"" + outputNcml + "\"");

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
            patterns[k] = TimeToken.toDateFormatPattern(patterns[k]);
            log.debug("\n--------------------------\n" + patterns[k]);

            df = new SimpleDateFormat(patterns[k]);
            result = df.format(cal.getTime());

            assert result.equals(answers[k]) : result + " != " + answers[k];
            log.debug(result + " = " + answers[k]);
        }

    }


    public static Map<String, Long> getTargetFiles(String host, String baseDir, String filePattern, String dirPattern, long startTime,
        long endTime) throws IOException {
        Map<String, Long> result = null;

        EasyFtp ftp = null;
        try {
            ftp = new EasyFtp(host);
            ftp.cd(baseDir);
            result = getTargetFiles(ftp, filePattern, dirPattern, startTime, endTime);
        } finally {
            if (null != ftp) {
                ftp.close();
            }
        }


        return result;
    }


    private static Map<String, Long> getTargetFiles(EasyFtp ftp, String filePattern, String dirPattern, long startTime, long endTime)
        throws IOException {

        List<String> dirs = getTargetDirs(dirPattern, startTime, endTime);

        if (log.isDebugEnabled()) {
            log.debug("***Receieved dirs:");
            for (String dir : dirs) {
                log.debug("\t" + dir);
            }
        }

        /* Gather inputs to the matching mechanism */
        Map<String, Long> result = new TreeMap<String, Long>();
        String regex = TimeToken.toRegexMatchPattern(filePattern);
        List<TimeToken> tokens = TimeToken.parseTokens(filePattern);
        List<String> filenames = null;


        /* MATCHING: Start checking which filenames are within the time bounds */
        Pattern p = Pattern.compile(regex);
        for (String dir : dirs) {
            // log.debug("Checking directory: " + dir + "\n******************************\n");
            // log.debug(ftp.nlist(dir));

            filenames = Arrays.asList(ftp.nlist(dir, regex).split("[\r\n]+"));
            for (String filename : filenames) {
                // log.debug(filename);

                Matcher m = p.matcher(filename);

                // if (null == m || !m.matches()) {
                // break;
                // }

                /* Determine what Date/Time this file represents */
                /* Start the calendar at startTime so that if fields are missing
                 * from the given filename we assume that those fields are within
                 * an acceptable range */
                Calendar cal = createUtcCal(startTime);
                while (m.find()) {
                    for (int i = 1; i <= m.groupCount(); i++) {
                        String stime = filename.substring(m.start(i), m.end(i));
                        int itime = Integer.parseInt(stime);

                        /* Since months are indexed by 0 - subtract 1 from itime if
                         * the calendar field is MONTHS
                         */
                        int calField = tokens.get(i - 1).getType().getCalendarField();
                        if (calField == Calendar.MONTH) {
                            --itime;
                        }
                        cal.set(calField, itime);
                    }
                }


                /* DEBUGGING... */
                DateFormat df = SimpleDateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL);
                df.setTimeZone(TimeZone.getTimeZone("UTC"));
                // String outputTime = df.format(cal.getTime());
                // log.debug(filename + "  is set to: " + outputTime);

                /* Check if this file's Date/Time is within the requested time range */
                long fileTime = cal.getTimeInMillis();
                if (startTime <= fileTime && fileTime < endTime) {
                    result.put(dir + filename, fileTime);
                }

            }

        }


        if (log.isDebugEnabled()) {
            log.debug("***Receieved files:");
            for (String file : result.keySet()) {
                log.debug("\t" + file);
            }
        }

        return result;
    }


    public static List<String> getTargetDirs(String pattern, long startTime, long endTime) {
        List<String> results = new ArrayList<String>();

        if (null == pattern || pattern.isEmpty()) {
            results.add(pattern);
            return results;
        }

        /* Get a list of all tokens in the pattern string */
        List<TimeToken> tokens = TimeToken.parseTokens(pattern);
        if (log.isDebugEnabled()) {
            for (TimeToken t : tokens) {
                log.debug("Character: " + t.getCharacter() + "\t\tCount: " + t.getLength());
            }
        }

        /* If there are no tokens, simply return pattern as our target directory */
        if (tokens.size() == 0) {
            results.add(pattern);
            return results;
        }

        /* Create a DateFormat pattern with the given tokenized pattern */
        String dfPattern = TimeToken.toDateFormatPattern(pattern);


        /* Sort the list of tokens according to their ordinal value */
        TimeToken.sort(tokens);

        /* Uses the lowest ordinal value as an incrementor to generate dates between start date and end date,
         * This is possible because tokens are mapped to a calendar field via their TokenType */
        int incField = tokens.get(0).getType().getCalendarField();

        /* Iterate over the dates between startTime and endTime and use these to generate directory strings */
        Calendar startCal = createUtcCal(startTime);
        Calendar endCal = createUtcCal(endTime);
        DateFormat sdf = new SimpleDateFormat(dfPattern);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        String result = "";
        do {
            result = sdf.format(startCal.getTime());
            /* Checking results.contains() is very expensive here, DONT DO IT */
            results.add(result);

            startCal.add(incField, 1);
        } while (startCal.compareTo(endCal) <= 0);



        return results;
    }

    
    public static void generateNcml(String output, Map<String, Long> filemap, String dimension) throws IOException {
        generateNcml(new File(output), filemap, dimension, null);
    }

    
    public static void generateNcml(String output, Map<String, Long> filemap, String dimension, String ncmlMask) throws IOException {
        generateNcml(new File(output), filemap, dimension, ncmlMask);
    }


    public static void generateNcml(File outFile, Map<String, Long> filemap, String dimension) throws IOException {
        generateNcml(outFile, filemap, dimension, null);
    }
    
    
    public static void generateNcml(File outFile, Map<String, Long> filemap, String dimension, String ncmlMask) throws IOException {

        /* Restructure the filelist so we can see what files have equal values (timesteps) */
        Map<Long, List<String>> groupings = new TreeMap<Long, List<String>>();
        for (Entry<String, Long> entry : filemap.entrySet()) {
            String key = entry.getKey();
            Long value = entry.getValue();

            if (groupings.containsKey(value)) {
                groupings.get(value).add(key);
            } else {
                List<String> listValue = new ArrayList<String>();
                listValue.add(key);
                groupings.put(value, listValue);
            }

        }


        /* Create a union for every grouping of files which share a timestamp and cache
         * the resultant output, otherwise, simply cache the filename if they don't
         * share a timestamp */
        List<String> unions = new ArrayList<String>();
        for (List<String> grouping : groupings.values()) {
            if (grouping.size() > 1) {
                unions.add(generateNcml_union(grouping));
            } else if (grouping.size() == 1) {
                unions.add(grouping.get(0));
            } else {
                throw new AssertionError("Internal error -- filelist should not be empty");
            }
        }


        /* Now use the given value for "output" to create an NCML join over the "time" dimension */
        /* TODO: dynamically determine time dimension name (provide in context??) */
        String ncml = generateNcml_join(unions, dimension);

        
        /* Extract the contents of the ncmlMask and insert into the resultant ncml: */
        if (null != ncmlMask && !ncmlMask.isEmpty()) {
            
            String ncmlMaskContents = null;
            /* Remove header and footer xml tags */
            ncmlMaskContents = ncmlMask.replaceAll("(?m)(^[\\s]*<netcdf(?=[^>]*?xmlns)[^>]*?>)|(</netcdf>[\\s]*)$", "");
            
            /* Realign indentation */
//            while (ncmlMaskContents.matches("(?m)[\\r\\n]?([ ]{3}|\\t).*")) {
                ncmlMaskContents = ncmlMaskContents.replaceAll(new StringBuilder("^(").append(TAB_STR).append("||\\t)").toString(), "");
//            }
            ncml = new StringBuilder(ncml).append(NEW_LINE).append(ncmlMaskContents).toString();
        }
        
        
        /* Write the output */
        FileWriter fw = new FileWriter(outFile);
        try {
            fw.write(applyNcmlHeadersFooters(ncml));
            fw.flush();
        } finally {
            try {
                fw.close();
            } catch (IOException ex) {
                /* TODO: log ex */
                /* NO-OP */
            }
        }
    }


    private static String generateNcml_join(List<String> contentList, String dimension) throws IOException {

        /* TODO: Is it safe to assume the time dimensions name is "time"? */
        /* TODO: Dynamically create the title attribute -- or does this matter */

        StringBuilder sb = new StringBuilder();

        sb.append("<aggregation dimName=\"" + dimension + "\" type=\"joinExisting\">");

        for (String content : contentList) {
            if (content.startsWith("<")) {
                /* Treat the input as a nested tag (presumably aggregation) */
                sb.append(NEW_LINE).append(TAB_STR).append("<netcdf>");
                sb.append(NEW_LINE).append(content.replaceAll("(?m)^", TAB_STR + TAB_STR));
                sb.append(NEW_LINE).append(TAB_STR).append("</netcdf>");

            } else {
                /* Treat the input as a file location */
                sb.append(NEW_LINE).append(TAB_STR).append("<netcdf location=\"");
                sb.append(content);
                sb.append("\"/>");

            }
        }
        sb.append(NEW_LINE).append("</aggregation>");


        return sb.toString();
    }


    private static String generateNcml_union(List<String> contentList) throws IOException {
        /* TODO: Is it safe to assume the time dimensions name is "time"? */
        /* TODO: Dynamically create the title attribute -- or does this matter */

        StringBuilder sb = new StringBuilder();

        sb.append("<aggregation type=\"union\">");

        for (String content : contentList) {
            if (content.startsWith("<")) {
                /* Treat the input as a nested tag (presumably aggregation) */
                sb.append(NEW_LINE).append(TAB_STR).append("<netcdf>");
                sb.append(NEW_LINE).append(content.replaceAll("(?m)^", TAB_STR + TAB_STR));
                sb.append(NEW_LINE).append(TAB_STR).append("</netcdf>");

            } else {
                /* Treat the input as a file location */
                sb.append(NEW_LINE).append(TAB_STR).append("<netcdf location=\"");
                sb.append(content);
                sb.append("\"/>");

            }
        }
        sb.append(NEW_LINE).append("</aggregation>");


        return sb.toString();
    }


    private static String applyNcmlHeadersFooters(String ncml) {
        /* Put a tab before every line in the given string
         * -- "^" means line begining in regex -- */
        StringBuilder sb = new StringBuilder(ncml.replaceAll("(?m)^", TAB_STR));

        sb.insert(0, NEW_LINE);
        sb.insert(0, "<netcdf xmlns=\"http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2\">");
        sb.append(NEW_LINE).append("</netcdf>");

        return sb.toString();
    }


    public static Calendar createUtcCal() {
        return createUtcCal(0);
    }


    public static Calendar createUtcCal(long millis) {
        Calendar result = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        result.setTimeInMillis(millis);
        return result;
    }


    public static Calendar createUtcCal(Date time) {
        Calendar result = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        result.setTime(time);
        return result;
    }


    public static String getFilename(String filepath) {
        return filepath.replaceFirst("^.+/(?=.+)", "");
    }


    public static String getParent(String filepath) {
        return filepath.replaceFirst("/[^/]+$", "/");
    }

    public static class UrlParser {
        private final String fullUrl;
        private final String protocol;
        private final String host;
        private final String directory;
        private final String file;

        /**
         * 
         */
        public UrlParser(String url) {
            Pattern p = Pattern.compile("([a-zA-Z]+)://([^/]+)(/?.*?/?)([^/]*$)");
            Matcher m = p.matcher(url);

            if (m.find()) {

                fullUrl = url;
                protocol = m.group(1);
                host = m.group(2);
                directory = m.group(3);
                file = m.group(4);

            } else {
                throw new IllegalArgumentException("Not Parsable: The given URL is not in the expected format");
            }
        }

        public String getFullUrl() {
            return fullUrl;
        }

        public String getProtocol() {
            return protocol;
        }

        public String getHost() {
            return host;
        }

        public String getDirectory() {
            return directory;
        }

        public String getFile() {
            return file;
        }




    }

}
