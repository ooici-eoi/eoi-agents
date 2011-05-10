/*
 * File Name:  TestFtp.java
 * Created on: May 3, 2011
 */
package net.ooici.eoi.ftp;


import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
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
    private static final String NEW_LINE = System.getProperty("line.separator");
    private static final String TAB_STR = "   ";


    public static void main(String[] args) throws IOException {
        EasyFtp ftp = null;


        /* Testing FTP access */
        // ftp = new EasyFtp("podaac.jpl.nasa.gov");
        // ftp.cd("/allData/ghrsst/data/L2P/MODIS_A/JPL/");
        // System.out.println(ftp.list("2011/001/", ".*A[0-9]+?0000.*\\.bz2$"));
        // ftp.close();


        /* Testing TOKENS */
        // String pattern = "%yyyy%/%DDD%/%yy%/%DD%/%dd%/%yyyy%";
        // List<Token> tokens = Token.getTokens(pattern);
        // for (Token t : tokens) {
        // System.out.println(t);
        // }
        //
        // System.out.println("************************");
        //
        // Token.sort(tokens);
        // for (Token t : tokens) {
        // System.out.println(t);
        // }

        /* Testings GenerateDateFormatPattern() */
        // testGenerateDateFormatPattern();
        // System.exit(0);

        /* Testing isParsablePattern */
        // String p = "%yyy%sss%ddd";
        // System.out.println(TimeToken.isParsable(p));
        // System.exit(0);



        /* Testing getTargetDirs */
        // String pattern = "%yyyy%/%MM%/(%DDD%|%dd%)%HH%.%mm%/";
        //
        // if (! TimeToken.isParsable(pattern)) {
        // System.out.println("Pattern is invalid!");
        // }
        // System.out.println("\n\n*********************************\n\n");
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
        // System.out.println("End Time = " + ((System.currentTimeMillis() - debugStartTime) / 1000.0));
        // for (String dir : dirs) {
        // System.out.println(">>>>  " + dir);
        // }


        // testGetTargetFiles();

        // sandboxTest();

        // testUnzipGz();

        testGenerateNcml();

    }


    private static void testGenerateNcml() throws IOException {

        String output = "/Users/tlarocque/Downloads/ooici/ncml/hycom-union-and-join.ncml";
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



        System.out.println("Generating union/join NCML file...");
        generateNcml(output, filemap, "MT");
        System.out.println("... COMPLETE!");

        System.out.println("\n\nReference: " + output);
    }


    private static void testUnzipGz() throws IOException {

        String filepath = "/Users/tlarocque/Downloads/p5000008.nc.gz";

        List<String> outFiles = EasyFtp.unzip(filepath);

        for (String outFile : outFiles) {

            System.out.println(outFile);
        }




    }

    
    private static void sandboxTest() {


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


        System.out.println("\n\n\n*************************************************************\n\n\n");
        // int minutes_ctr = 13 * 60 + 50;
        // int day_in_minutes = 24 * 60;
        // Format fmt = new DecimalFormat("#00");

        // Pattern p = Pattern.compile("([0-9]{4})(?=00\\.L2)");
        // Matcher m = null;
        // int totalError = 0;
        for (String file : remoteFiles.keySet()) {
            System.out.println(file);
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
        System.out.println("\n\n\n*************************************************************\n\n\n");
        System.out.println("Total files: " + remoteFiles.size());
        // System.out.println("Total extras: " + totalError);

        System.out.println("\n\nDOWNLOADING...");
        EasyFtp ftp = new EasyFtp(host);
        ftp.cd(baseDir);
        Map<String, Long> localFiles = new TreeMap<String, Long>();

        for (String key : remoteFiles.keySet()) {
            /* Download the file */
            String download = ftp.download(key, "/Users/tlarocque/Downloads/ooici/");
            System.out.println("\n\n" + download);


            /* Test unzipping... */
            String unzipped = EasyFtp.unzip(download).get(0);
            System.out.println(unzipped);


            /* Insert the new output name back into the map */
            Long val = remoteFiles.get(key);
            localFiles.put(unzipped, val);
        }

        /* Try generating an NCML (time aggregation) */
        String outputNcml = "/Users/tlarocque/Downloads/ooici/ncml/glider_temp.ncml";
        generateNcml(outputNcml, localFiles, "MT");

        System.out.println("\n\nGenerated NCML aggregation...\n\t\"" + outputNcml + "\"");

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
            System.out.println("\n--------------------------\n" + patterns[k]);

            df = new SimpleDateFormat(patterns[k]);
            result = df.format(cal.getTime());

            assert result.equals(answers[k]) : result + " != " + answers[k];
            System.out.println(result + " = " + answers[k]);
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
        ;

        /* Gather inputs to the matching mechanism */
        Map<String, Long> result = new TreeMap<String, Long>();
        String regex = TimeToken.toRegexMatchPattern(filePattern);
        List<TimeToken> tokens = TimeToken.parseTokens(filePattern);
        List<String> filenames = null;


        /* MATCHING: Start checking which filenames are within the time bounds */
        for (String dir : dirs) {
            // System.out.println("Checking directory: " + dir + "\n******************************\n");
            // System.out.println(ftp.nlist(dir));

            filenames = Arrays.asList(ftp.nlist(dir, regex).split("[\r\n]+"));
            for (String filename : filenames) {
                // System.out.println(filename);

                Pattern p = Pattern.compile(regex);
                Matcher m = p.matcher(filename);

                // if (null == m || !m.matches()) {
                // break;
                // }

                /* Determine what Date/Time this file represents */
                Calendar cal = createUtcCal();
                while (m.find()) {
                    for (int i = 1; i <= m.groupCount(); i++) {
                        String stime = filename.substring(m.start(i), m.end(i));
                        int itime = Integer.parseInt(stime);

                        cal.set(tokens.get(i - 1).getType().getCalendarField(), itime);
                    }
                }


                /* DEBUGGING... */
                DateFormat df = SimpleDateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL);
                df.setTimeZone(TimeZone.getTimeZone("UTC"));
                // String outputTime = df.format(cal.getTime());
                // System.out.println(filename + "  is set to: " + outputTime);

                /* Check if this file's Date/Time is within the requested time range */
                long fileTime = cal.getTimeInMillis();
                if (startTime <= fileTime && fileTime < endTime) {
                    result.put(dir + filename, fileTime);
                }

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
        for (TimeToken t : tokens) {
            System.out.println("Character: " + t.getCharacter() + "\t\tCount: " + t.getLength());
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



        /* Write the output */
        FileWriter fw = new FileWriter(output);
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


    public static String getFilename(String filepath) {
        return filepath.replaceFirst("^.+/(?=.+)", "");
    }


    public static String getParent(String filepath) {
        return filepath.replaceFirst("/[^/]+$", "/");
    }


}
