/*
 * File Name:  FtpDataSourceCrawler.java
 * Created on: Jun 23, 2011
 */
package net.ooici.eoi.crawler.impl;

import java.io.IOException;

import net.ooici.eoi.crawler.AccessClient;
import net.ooici.eoi.crawler.DataSourceCrawler;

/**
 * TODO Add class comments
 * 
 * @author tlarocque
 * @version 1.0
 */
public class FtpDataSourceCrawler extends DataSourceCrawler {

    /**
     * 
     * @param host
     * @param baseDir
     * @param filePattern
     * @param dirPattern
     */
    public FtpDataSourceCrawler(String host, String baseDir, String filePattern, String dirPattern) {
        super(host, baseDir, filePattern, dirPattern);
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
    public FtpDataSourceCrawler(String host, String user, String pasw, String baseDir, String filePattern, String dirPattern) {
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
            result = new FtpAccessClient(host);
        } else {
            result = new FtpAccessClient(host, user, pasw);
        }
        
        return result;
    }


}
