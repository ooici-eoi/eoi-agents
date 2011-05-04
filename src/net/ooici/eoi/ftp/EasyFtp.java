/*
 * File Name:  GenericTest.java
 * Created on: May 3, 2011
 */
package net.ooici.eoi.ftp;

import java.io.IOException;

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

    private final FTPClient ftp;
    private String cddir = "/";
    private static final String NEW_LINE = System.getProperty("line.separator");
    
    
    public static void main(String... args) throws IOException {
        
//        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
//        cal.setTimeInMillis(0);
//        cal.set(1970, Calendar.JANUARY, 1);
//        
//        DateFormat fmt = SimpleDateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL);
//        fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
//        System.out.println(fmt.format(cal.getTime()));
//        
//        
//        cal.add(Calendar.SECOND, 1301159480);
//        cal.add(Calendar.MILLISECOND, 955);
//        System.out.println(fmt.format(cal.getTime()));
//        
//        System.out.println(new SimpleDateFormat("'DAY OF YEAR: 'DDD").format(cal.getTime()));
//        
//        
//        cal.add(Calendar.DAY_OF_YEAR, -489);
//        System.out.println(fmt.format(cal.getTime()));
//        System.out.println(new SimpleDateFormat("'DAY OF YEAR: 'DDD").format(cal.getTime()));
        
        
        EasyFtp ftp = new EasyFtp("ftp7300.nrlssc.navy.mil");
        ftp.cd("/pub/smedstad/ROMS/");
        System.out.println(ftp.list(".", ".*sal.*"));
        
    }
    
    
    public EasyFtp(final String host) throws IOException {
        this(host, "anonymous", "");
    }
    
    
    public EasyFtp(final String host, final String user, final String pasw) throws IOException {
        ftp = new FTPClient();
        ftp.addProtocolCommandListener(new ProtocolCommandListener() {
            
            @Override
            public void protocolReplyReceived(ProtocolCommandEvent pce) {
                System.out.print(new StringBuilder("<<<---@@@ RECEIVE: ").append(pce.getCommand()).append(": ").append(pce.getMessage()).toString());
            }
            
            @Override
            public void protocolCommandSent(ProtocolCommandEvent pce) {
                System.out.print(new StringBuilder("<<<---@@@ RECEIVE: ").append(pce.getCommand()).append(": ").append(pce.getMessage()).toString());
            }
        });
        
        
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
//        String wd = fixdir(loc);
//        FTPFile[] files = ftp.listFiles(loc);
        String[] files = ftp.listNames(loc);
//        String filename = null;
        for (String filename : files) {
//            filename = file.getName();
            if (null != filename && (regex == null || filename.matches(regex)))
                result.append(filename).append(NEW_LINE);
//            result.append(file.file.getRawListing()).append(NEW_LINE);
        }
        
        if (result.length() >= NEW_LINE.length())
            result.delete(result.length() - NEW_LINE.length(), result.length());
        
        
        return result.toString();
    }
    
    
    private String fixdir(final String loc) {
        String result = "";
        if (null == loc || loc.isEmpty()) {
            result = cddir;
        } else if (! loc.startsWith("/")) {
            result = new StringBuilder(cddir).append("/").append(loc).toString();
        } else {
            result = loc;
        }
        
        return result;
    }
    
    
    public void close() {
        if (null != ftp) {
            try {
                ftp.logout();
            } catch(IOException ex) {
                // NO-OP
            }
            try {
                ftp.disconnect();
            } catch(IOException ex) {
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


/*

     FTPClient ftp = new FTPClient();
           ftp.addProtocolCommandListener(new ProtocolCommandListener() {

                @Override
                public void protocolCommandSent(ProtocolCommandEvent pce) {
//                    System.out.println("--->>>SENT: " + pce.getCommand() + ": " + pce.getMessage() + "--->>>");
                }

                @Override
                public void protocolReplyReceived(ProtocolCommandEvent pce) {
//                    System.out.println("<<<---RECEIVE: " + pce.getCommand() + ": " + pce.getMessage() + "<<<---");
                }
            });

            boolean error = false;
            try {
                ftp.connect("ftp7300.nrlssc.navy.mil");
                ftp.enterLocalPassiveMode();
                ftp.login("anonymous", "");
                ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
                System.out.println("reply: " + ftp.getReplyString());
                int reply = ftp.getReplyCode();
                if (!FTPReply.isPositiveCompletion(reply)) {
                    ftp.disconnect();
                    System.err.println("FTP server refused connection.");
                    System.exit(1);
                }

                ftp.changeWorkingDirectory("pub/smedstad/ROMS/");

                File out = new File("out/ftp");
                List<String> files = Arrays.asList(out.list());

                System.out.println("***names***");
                for (String s : ftp.listNames()) {
                    System.out.println(s);
                    //TODO: check against the regex for this FTP site, if qualify, check if we "have it" (time based I would think), then download and process file
//                    if (s.equals("909_archv.2011040718_2011040400_idp_EastCst1.nc")) {
                    if (files.contains(s)) {
                        System.out.println("Already have it!");
                        continue;
                    }
                    System.out.print("Downloading...");
                    OutputStream fos = new FileOutputStream(new File(out, s));
                    try {
                        if (ftp.retrieveFile(s, fos)) {
                            System.out.println("Success! :-)");
                        } else {
                            System.out.println("Fail! :-(");
                        }
                    } finally {
                        fos.flush();
                        fos.close();
                    }
//                    }
                }

                ftp.logout();
            } catch (Exception e) {
                error = true;
                e.printStackTrace();
            } finally {
                if (ftp.isConnected()) {
                    try {
                        ftp.disconnect();
                    } catch (IOException ioe) {
                        // do nothing
                    }
                }
                System.exit(error ? 1 : 0);
            }


*/