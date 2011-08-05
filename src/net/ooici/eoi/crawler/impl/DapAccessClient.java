/*
 * File Name:  GenericTest.java
 * Created on: May 3, 2011
 */
package net.ooici.eoi.crawler.impl;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.ooici.eoi.crawler.AccessClient;
import net.ooici.eoi.crawler.DataSourceCrawler;
import thredds.catalog.InvCatalogRef;
import thredds.catalog.InvDataset;
import thredds.catalog.ServiceType;
import thredds.catalog.crawl.CatalogCrawler;


/**
 * TODO Add class comments
 * 
 * @author tlarocque
 * @version 1.0
 */
public class DapAccessClient implements AccessClient {

    /** Static Fields */
    static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DapAccessClient.class);
    private static final String NEW_LINE = System.getProperty("line.separator");
    
    /** Instance Fields */
    private String host;
    private String cddir = "/";
    CatalogViewer catalogViewer = new CatalogViewer();


    
    
    public static void main(String[] args) throws IOException {
        AccessClient client = null;
        
//        "ftp://podaac.jpl.nasa.gov/allData/ghrsst/data/L2P/MODIS_A/JPL/";
//        client = new FtpAccessClient("podaac.jpl.nasa.gov");
//        System.out.println(client.pwd());
//        System.out.println(client.cd("/allData/ghrsst/data/L2P/MODIS_A/JPL/"));
//        System.out.println(client.pwd());
//        System.out.println(client.cd("2011/"));
//        System.out.println(client.pwd());
//        System.out.println(client.cd("/allData/ghrsst/data/L2P/MODIS_A/JPL/"));
//        System.out.println(client.pwd());
//        client.close();
        
        client = new DapAccessClient("nomads.ncdc.noaa.gov");
        System.out.println(client.pwd());
        System.out.println(client.cd("/thredds/dodsC/gfs-hi/"));
        System.out.println(client.pwd());
        System.out.println(client.cd("201106"));
        System.out.println(client.pwd());
        
        System.out.println(client.list("20110622"));
        client.close();
        
        
        
        
        
        
//        System.out.println(client.list("http://nomads.ncdc.noaa.gov/thredds/dodsC/gfs-hi/201106/20110622/catalog.xml"));
        
        
        
        
        
    }
    
    
    
    
    
    /**
     * 
     * 
     * TODO Add class comments
     * 
     * @author tlarocque
     * @version 1.0
     */
    class CatalogViewer implements CatalogCrawler.Listener {

        final List<String> crawlList = new ArrayList<String>();
        final CatalogCrawler crawler = new CatalogCrawler(CatalogCrawler.USE_ALL_DIRECT, false, this);
        
        public List<String> list(String catalog) {
            crawlList.clear();
            crawler.crawl(catalog, null, null, null);
            return new ArrayList<String>(crawlList);
        }
        
        @Override
        public void getDataset(InvDataset id, Object o) {
            String urlPathname = id.getAccess(ServiceType.OPENDAP).getStandardUrlName();
//            System.out.println("Dataset2: " + urlPathname);
            crawlList.add(urlPathname);
        }
        
        @Override
        public boolean getCatalogRef(InvCatalogRef icr, Object o) {
            System.out.println("CatalogRef: " + icr);
            return true;
        }
        
    }
    
    
    
    public DapAccessClient(final String host) throws IOException {
        this(host, "", "");
        /* TODO: support user/pasw for DAP access if neccessary */
    }


    public DapAccessClient(final String host, final String user, final String pasw) throws IOException {
        /* Remove all trailing slashes (NPE is ok, since null host is not allowed) */
        this.host = DataSourceCrawler.removeTrailingSlashes(host);
    }

    
    /*
     * (non-Javadoc)
     * @see net.ooici.eoi.crawler.AccessClient#getHost()
     */
    @Override
    public String getHost() {
        return host;
    }
    
    
    /*
     * (non-Javadoc)
     * @see net.ooici.eoi.crawler.AccessClient#getProtocol()
     */
    @Override
    public String getProtocol() {
        return "http";
    }
    
    
    public String cd(String dir) throws IOException {
        if (null != dir && !dir.isEmpty()) {
            cddir = fixdir(dir);
        }
        return cddir;
    }


    public String pwd() throws IOException {
        return cddir;
    }
    
    
    private String getFullPath(String loc) {
        return new StringBuilder("http://").append(host).append(loc).toString();
        
    }
    

    public String getFullCatalogPath(String loc) {
        /* Appends loc to the end of our currently cd'd directory (without changing that directory),
         * then appends ".catalog" to that locations full path
         */
        return getFullPath(fixdir(loc)) + "/catalog.xml";
    }

    
    public String list() throws IOException {
        return list(null);
    }


    public String list(final String loc) throws IOException {
        return list(loc, null);
    }


    public String list(final String loc, final String regex) throws IOException {
        StringBuilder result = new StringBuilder();
        
        List<String> filenames = catalogViewer.list(getFullCatalogPath(loc));
        for (String filename : filenames) {
            if (null != filename && (regex == null || filename.matches(regex)))
                result.append(filename).append(NEW_LINE);
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
        
        List<String> filenames = catalogViewer.list(getFullCatalogPath(loc));
        for (String filename : filenames) {
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
        /* If the given loc does not start with a slash, assume we want a relative directory */
        String result = "";
        if (!loc.startsWith("/")) {
            if (cddir.endsWith("/")) {
                result = new StringBuilder(cddir).append(loc).toString();
            } else {
                result = new StringBuilder(cddir).append('/').append(loc).toString();
            }
        } else {
            /* Otherwise just use the given directory */
            result = loc;
        }
        
        
        /* Use canonical path to remove instances of '.' and '..' in the pathname */
        if (null != result) {
            File file = new File(result);
            try {
                result = file.getCanonicalPath();
            } catch (IOException e) {
                result = file.getAbsolutePath();
            }
        }

        return result;
    }

    
    public void close() {
        /* nothing to do */
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
