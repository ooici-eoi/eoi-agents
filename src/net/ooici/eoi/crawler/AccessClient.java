/*
 * File Name:  AccessClient.java
 * Created on: Jun 23, 2011
 */
package net.ooici.eoi.crawler;

import java.io.IOException;

/**
 * TODO Add class comments
 * 
 * @author tlarocque
 * @version 1.0
 */
public interface AccessClient {

//    AccessClient create(String host) throws IOException;
//    AccessClient create(String host, String user, String pasw) throws IOException;
    
    String getHost();
    String getProtocol();
    
    String nlist() throws IOException;
    String nlist(String location) throws IOException;
    String nlist(String location, String regex) throws IOException;
    
    String list() throws IOException;
    String list(String location) throws IOException;
    String list(String location, String regex) throws IOException;
    
    String cd(String directory) throws IOException;
    String pwd() throws IOException;
    
//    String download(String filepath, String outputLocation) throws IOException;
//    String download(String filepath, String outputLocation, boolean deleteOnExit) throws IOException;
//    boolean download(String filepath, File output) throws IOException;
    
    void close();
}
