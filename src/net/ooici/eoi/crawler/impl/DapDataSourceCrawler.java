/*
 * File Name:  DapDataSourceCrawler.java
 * Created on: Jun 23, 2011
 */
package net.ooici.eoi.crawler.impl;

import java.io.IOException;

import net.ooici.eoi.crawler.AccessClient;
import net.ooici.eoi.crawler.DataSourceCrawler;
import net.ooici.eoi.crawler.util.UrlParser;

/**
 * TODO Add class comments
 * 
 * @author tlarocque
 * @version 1.0
 */
public class DapDataSourceCrawler extends DataSourceCrawler {
    
    /**
     * @param host
     * @param baseDir
     * @param filePattern
     * @param dirPattern
     */
    public DapDataSourceCrawler(String host, String baseDir, String filePattern, String dirPattern) {
        super(host, baseDir, filePattern, dirPattern);
    }
    
    public static void main(String[] args) {
        String url = "http://nomads.ncdc.noaa.gov/thredds/dodsC/gfs-hi/201106/catalog.xml";
        UrlParser p = new UrlParser(url);
        System.out.println(p.getHost());
        System.out.println(p.getDirectory());
        
        System.out.println("http://" + p.getHost() + p.getDirectory() + "catalog.xml");
        System.out.println(url);
    }
    
    /**
     * 
     * @param host
     * @param user
     * @param pasw
     * @param baseDir
     * @param filePattern
     * @param dirPattern
     */
    public DapDataSourceCrawler(String host, String user, String pasw, String baseDir, String filePattern, String dirPattern) {
        super(host, user, pasw, baseDir, filePattern, dirPattern);
    }
    
    
    /* (non-Javadoc)
     * @see net.ooici.eoi.crawler.DataSourceCrawler#getAccessClient(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public AccessClient createAccessClient(String host, String user, String pasw) throws IOException {
        AccessClient result = null;
        if (null == user && null == pasw) {
            /* Delegate to the 1 field constructor to let the access client choose default params for user and password */
            result = new DapAccessClient(host);
        } else {
            result = new DapAccessClient(host, user, pasw);
        }
        
        return result;
    }

    
}
