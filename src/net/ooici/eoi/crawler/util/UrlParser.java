/*
 * File Name:  UrlParser.java
 * Created on: Jun 23, 2011
 */
package net.ooici.eoi.crawler.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TODO Add class comments
 * 
 * @author tlarocque
 * @version 1.0
 */
public class UrlParser {
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