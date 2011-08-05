/*
 * File Name:  TestFtp.java
 * Created on: May 3, 2011
 */
package net.ooici.eoi.crawler;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ooici.eoi.crawler.util.TimeToken;
import net.ooici.eoi.datasetagent.AgentUtils;


/**
 * TODO Add class comments
 * 
 * @author tlarocque
 * @version 1.0
 */
public abstract class DataSourceCrawler {

    /** Static Fields */
    static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DataSourceCrawler.class);
    private static final String NEW_LINE = System.getProperty("line.separator");
    private static final String TAB_STR = "    ";
    
    
    /** Instance Fields */
    final String host;
    String user;
    String pasw;
    final String baseDir;
    final String filePattern;
    final String dirPattern;

    
    
    
    public abstract AccessClient createAccessClient(String host, String user, String pasw) throws IOException;
    public AccessClient createAccessClient() throws IOException {
        return createAccessClient(host, user, pasw);
    }
    
    

    
    protected DataSourceCrawler() {
        throw new UnsupportedOperationException("Constructor not supported: Cannot create an immutable DataSourceCrawler without host, base directory and pattern fields");
    }
    
    
    /**
     * 
     */
    public DataSourceCrawler(String host, String baseDir, String filePattern, String dirPattern) {
        this(host, null, null, baseDir, filePattern, dirPattern);
    }

    
    /**
     * 
     */
    public DataSourceCrawler(String host, String user, String pasw, String baseDir, String filePattern, String dirPattern) {
        this.host = host;
        this.user = user;
        this.pasw = pasw;
        this.baseDir = baseDir;
        this.filePattern = filePattern;
        this.dirPattern = dirPattern;
    }
    

    
    
    public Map<String, Long> getTargetFileNames(long startTime, long endTime) throws IOException {
        return getTargetFiles(startTime, endTime, TargetFileDisplayMode.FILENAME_ONLY);
    }

    
    public Map<String, Long> getTargetFilesRelativeToBase(long startTime, long endTime) throws IOException {
        return getTargetFiles(startTime, endTime, TargetFileDisplayMode.RELATIVE_TO_BASE);
    }
    
    
    public Map<String, Long> getTargetFilesFullPath(long startTime, long endTime) throws IOException {
        return getTargetFiles(startTime, endTime, TargetFileDisplayMode.FULL_PATH);
    }
    
    
    private Map<String, Long> getTargetFiles(long startTime, long endTime, TargetFileDisplayMode mode) throws IOException {
        Map<String, Long> result = null;

        AccessClient client = createAccessClient(host, user, pasw);
        try {
            client.cd(baseDir);
            result = getTargetFiles(client, startTime, endTime, mode);
        } finally {
            if (null != client) {
                client.close();
            }
        }


        return result;
    }

    
    private enum TargetFileDisplayMode {
        FULL_PATH, RELATIVE_TO_BASE, FILENAME_ONLY
    }
    
    
    private Map<String, Long> getTargetFiles(AccessClient client, long startTime, long endTime, TargetFileDisplayMode mode) throws IOException {

        List<String> dirs = getTargetDirs(startTime, endTime);

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

            filenames = Arrays.asList(client.nlist(dir, regex).split("[\r\n]+"));
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
                Calendar cal = AgentUtils.createUtcCal(startTime);
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
                    
                    switch (mode) {
                        case FULL_PATH:
                            result.put(new StringBuilder(client.getProtocol())
                                           .append("://")
                                           .append(client.getHost())
                                           .append(client.pwd())
                                           .append('/')
                                           .append(dir)
                                           .append(filename)
                                           .toString(),
                                       fileTime);
                            break;
                        case RELATIVE_TO_BASE:
                            result.put(dir + filename, fileTime);
                            break;
                        case FILENAME_ONLY:
                            result.put(filename, fileTime);
                            break;
                            
                    }
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


    public List<String> getTargetDirs(long startTime, long endTime) {
        List<String> results = new ArrayList<String>();

        if (null == dirPattern || dirPattern.isEmpty()) {
            results.add(dirPattern);
            return results;
        }

        /* Get a list of all tokens in the pattern string */
        List<TimeToken> tokens = TimeToken.parseTokens(dirPattern);
        if (log.isDebugEnabled()) {
            for (TimeToken t : tokens) {
                log.debug("Character: " + t.getCharacter() + "\t\tCount: " + t.getLength());
            }
        }

        /* If there are no tokens, simply return pattern as our target directory */
        if (tokens.size() == 0) {
            results.add(dirPattern);
            return results;
        }

        /* Create a DateFormat pattern with the given tokenized pattern */
        String dfPattern = TimeToken.toDateFormatPattern(dirPattern);


        /* Sort the list of tokens according to their ordinal value */
        TimeToken.sort(tokens);

        /* Uses the lowest ordinal value as an incrementor to generate dates between start date and end date,
         * This is possible because tokens are mapped to a calendar field via their TokenType */
        int incField = tokens.get(0).getType().getCalendarField();

        /* Iterate over the dates between startTime and endTime and use these to generate directory strings */
        Calendar startCal = AgentUtils.createUtcCal(startTime);
        Calendar endCal = AgentUtils.createUtcCal(endTime);
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
            // while (ncmlMaskContents.matches("(?m)[\\r\\n]?([ ]{3}|\\t).*")) {
            ncmlMaskContents = ncmlMaskContents.replaceAll(new StringBuilder("^(").append(TAB_STR).append("||\\t)").toString(), "");
            // }
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
    
    /* TODO: may need to add a method to create an FMRC NCML -- must discuss with Stuebe */

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
    



    public static String removeTrailingSlashes(String dir) {
        return dir.replaceFirst("[/]*$", "");
    }
    
    
}
