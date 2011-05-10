/*
 * File Name:  GenericTest.java
 * Created on: May 3, 2011
 */
package net.ooici.eoi.ftp;


import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.net.ProtocolCommandEvent;
import org.apache.commons.net.ProtocolCommandListener;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;


/**
 * TODO Add class comments
 * 
 * @author tlarocque
 * @version 1.0
 */
public class EasyFtp {

    private boolean DEBUG = false;

    private final FTPClient ftp;
    private String cddir = "/";
    private static final String NEW_LINE = System.getProperty("line.separator");


    public static void main(String... args) throws IOException {

        // Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        // cal.setTimeInMillis(0);
        // cal.set(1970, Calendar.JANUARY, 1);
        //
        // DateFormat fmt = SimpleDateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL);
        // fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        // System.out.println(fmt.format(cal.getTime()));
        //
        //
        // cal.add(Calendar.SECOND, 1301159480);
        // cal.add(Calendar.MILLISECOND, 955);
        // System.out.println(fmt.format(cal.getTime()));
        //
        // System.out.println(new SimpleDateFormat("'DAY OF YEAR: 'DDD").format(cal.getTime()));
        //
        //
        // cal.add(Calendar.DAY_OF_YEAR, -489);
        // System.out.println(fmt.format(cal.getTime()));
        // System.out.println(new SimpleDateFormat("'DAY OF YEAR: 'DDD").format(cal.getTime()));


        EasyFtp ftp = new EasyFtp("ftp7300.nrlssc.navy.mil");
        ftp.cd("/pub/smedstad/ROMS/");
        System.out.println(ftp.list(".", ".*sal.*"));

    }


    public EasyFtp(final String host) throws IOException {
        this(host, "anonymous", "");
    }


    public EasyFtp(final String host, final String user, final String pasw) throws IOException {
        ftp = new FTPClient();
        if (DEBUG) {
            ftp.addProtocolCommandListener(new ProtocolCommandListener() {

                @Override
                public void protocolReplyReceived(ProtocolCommandEvent pce) {
                    System.out.print(new StringBuilder("<<<---@@@ RECEIVE: ").append(pce.getCommand()).append(": ")
                        .append(pce.getMessage()).toString());
                }

                @Override
                public void protocolCommandSent(ProtocolCommandEvent pce) {
                    System.out.print(new StringBuilder("<<<---@@@ RECEIVE: ").append(pce.getCommand()).append(": ")
                        .append(pce.getMessage()).toString());
                }
            });
        }


        boolean error = true;
        try {
            ftp.connect(host);
            ftp.enterLocalPassiveMode();
            ftp.login(user, pasw);
            ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
            System.out.println("reply: " + ftp.getReplyString());
            int reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                throw new IOException(new StringBuilder("FTP server refused connection.  Reply Code: ").append(reply).toString());
            }
            error = false;
        } finally {
            if (error) {
                close();
            }
        }
    }


    public String cd(String dir) throws IOException {
        String cdto = fixdir(dir);
        ftp.changeWorkingDirectory(cdto);
        cddir = ftp.printWorkingDirectory();
        return cddir;
    }


    public String pwd() throws IOException {
        return ftp.printWorkingDirectory();
    }


    public String list() throws IOException {
        return list(null);
    }


    public String list(final String loc) throws IOException {
        return list(loc, null);
    }


    public String list(final String loc, final String regex) throws IOException {
        StringBuilder result = new StringBuilder();
        // String wd = fixdir(loc);
        // FTPFile[] files = ftp.listFiles(loc);
        String[] files = ftp.listNames(loc);
        // String filename = null;
        for (String filename : files) {
            // filename = file.getName();
            if (null != filename && (regex == null || filename.matches(regex)))
                result.append(filename).append(NEW_LINE);
            // result.append(file.file.getRawListing()).append(NEW_LINE);
        }

        if (result.length() >= NEW_LINE.length())
            result.delete(result.length() - NEW_LINE.length(), result.length());


        return result.toString();
    }


    public String nlist() throws IOException {
        return nlist(null);
    }


    public String nlist(final String loc) throws IOException {
        return nlist(loc, null);
    }


    public String nlist(final String loc, final String regex) throws IOException {
        StringBuilder result = new StringBuilder();
        String[] files = ftp.listNames(loc);
        for (String filename : files) {
            if (null != filename) {
                /* This method intentionally strips the final '/' off directory names */
                if (filename.endsWith("/")) {
                    filename = filename.substring(0, filename.length() - 1);
                }
                int start = filename.lastIndexOf("/");
                if (start >= 0 && ++start < filename.length())
                    filename = filename.substring(start);
                if (regex == null || filename.matches(regex))
                    result.append(filename).append(NEW_LINE);
            }
        }

        if (result.length() >= NEW_LINE.length())
            result.delete(result.length() - NEW_LINE.length(), result.length());


        return result.toString();
    }


    private String fixdir(final String loc) {
        String result = "";
        if (null == loc || loc.isEmpty()) {
            result = cddir;
        } else if (!loc.startsWith("/")) {
            result = new StringBuilder(cddir).append("/").append(loc).toString();
        } else {
            result = loc;
        }

        return result;
    }


    public String download(String fileLoc, String outDir) throws IOException {

        if (fileLoc.endsWith("/")) {
            throw new IllegalArgumentException("Given fileLoc specifies a directory");
        }

        int start = fileLoc.lastIndexOf("/");
        if (start == -1)
            start = 0;
        else
            start++;

        StringBuilder outPath = new StringBuilder(outDir);
        if (!outDir.endsWith(File.separator)) {
            outPath.append(File.separatorChar);
        }

        String fileName = fileLoc.substring(start, fileLoc.length());
        outPath.append(fileName);

        File out = new File(outPath.toString());
        download(fileLoc, out);



        return outPath.toString();
    }


    public boolean download(String fileLoc, File out) throws IOException {

        boolean result = false;

        FileOutputStream fos = new FileOutputStream(out);
        ;
        try {
            result = ftp.retrieveFile(fileLoc, fos);
        } finally {
            if (null != fos) {
                try {
                    fos.close();
                } catch (IOException e) {
                    /* TODO: log failure */
                    /* NO-OP */
                }
            }
        }


        return result;
    }


    public static List<String> unzip(String filepath) throws IOException {
        List<String> results = new ArrayList<String>();

        if (filepath.endsWith(".zip")) {
            results.addAll(unzip_zip(filepath));
        } else if (filepath.endsWith(".gz") || filepath.endsWith(".gzip")) {
            results.addAll(unzip_gzip(filepath));
        } else if (filepath.endsWith(".bz2") || filepath.endsWith(".bzip2")) {
            results.addAll(unzip_bzip2(filepath));
        } else {
            results.add(filepath);
        }

        return results;
    }


    public static List<String> unzip_zip(String filepath) throws IOException {
        List<String> result = new ArrayList<String>();
        ZipFile zf = new ZipFile(filepath);
        Enumeration<? extends ZipEntry> e = zf.entries();

        while (e.hasMoreElements()) {
            ZipEntry entry = (ZipEntry) e.nextElement();
            if (entry.isDirectory()) {
                System.out.println("Not recursing into directory: " + entry.getName());
                continue;
            }
            System.out.println("Unzipping: " + entry.getName());

            FileOutputStream fos = null;
            InputStream is = null;
            try {
                /* TODO: change outfile to the same dir as the input file */
                String outfile = "/Users/tlarocque/Downloads/ooici/" + entry.getName();
                fos = new FileOutputStream(outfile);
                is = new BufferedInputStream(zf.getInputStream(entry));

                final int BUFFER = 1024;
                byte[] bytes = new byte[BUFFER];
                int len = 0;
                while (-1 != (len = is.read(bytes, 0, BUFFER))) {
                    fos.write(bytes, 0, len);
                }
                result.add(outfile);

            } finally {
                if (null != fos)
                    try {
                        fos.flush();
                        fos.close();
                    } catch (IOException ex) { /* NO-OP */
                    }
                if (null != is)
                    try {
                        is.close();
                    } catch (IOException ex) { /* NO-OP */
                    }
            }
        }

        return result;
    }


    public static List<String> unzip_gzip(String filepath) throws IOException {
        List<String> result = new ArrayList<String>();

        System.out.println("Uncompressing (gzip): " + filepath);

        OutputStream fos = null;
        InputStream is = null;
        GZIPInputStream bzip = null;
        try {
            /* TODO: change outfile to the same dir as the input file */
            String outfile = filepath.replaceFirst("(\\.gz|\\.gzip)?$", "");
            if (outfile.equals(filepath)) {
                outfile = outfile + "(0)";
            }
            fos = new FileOutputStream(outfile);
            is = new FileInputStream(filepath);
            bzip = new GZIPInputStream(is);

            final int BUFFER = 1024;
            byte[] bytes = new byte[BUFFER];
            int len = 0;
            while (-1 != (len = bzip.read(bytes, 0, BUFFER))) {
                fos.write(bytes, 0, len);
            }
            result.add(outfile);

        } finally {
            if (null != fos)
                try {
                    fos.flush();
                    fos.close();
                } catch (IOException ex) { /* NO-OP */
                }
            if (null != bzip)
                try {
                    bzip.close();
                } catch (IOException ex) { /* NO-OP */
                }
            if (null != is)
                try {
                    is.close();
                } catch (IOException ex) { /* NO-OP */
                }
        }

        return result;
    }


    public static List<String> unzip_bzip2(String filepath) throws IOException {
        List<String> result = new ArrayList<String>();

        System.out.println("Uncompressing (bzip2): " + filepath);

        OutputStream fos = null;
        InputStream is = null;
        BZip2CompressorInputStream bzip = null;
        try {
            /* TODO: change outfile to the same dir as the input file */
            String outfile = filepath.replaceFirst("(\\.bz2|\\.bzip2)?$", "");
            if (outfile.equals(filepath)) {
                outfile = outfile + "(0)";
            }
            fos = new FileOutputStream(outfile);
            is = new FileInputStream(filepath);
            bzip = new BZip2CompressorInputStream(is);

            final int BUFFER = 1024;
            byte[] bytes = new byte[BUFFER];
            int len = 0;
            while (-1 != (len = bzip.read(bytes, 0, BUFFER))) {
                fos.write(bytes, 0, len);
            }
            result.add(outfile);

        } finally {
            if (null != fos)
                try {
                    fos.flush();
                    fos.close();
                } catch (IOException ex) { /* NO-OP */
                }
            if (null != bzip)
                try {
                    bzip.close();
                } catch (IOException ex) { /* NO-OP */
                }
            if (null != is)
                try {
                    is.close();
                } catch (IOException ex) { /* NO-OP */
                }
        }

        return result;
    }


    public void close() {
        if (null != ftp) {
            try {
                ftp.logout();
            } catch (IOException ex) {
                // NO-OP
            }
            try {
                ftp.disconnect();
            } catch (IOException ex) {
                // NO-OP
            }
        }
    }


    /* (non-Javadoc)
     * @see java.lang.Object#finalize()
     */
    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }
}
