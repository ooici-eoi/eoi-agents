/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package net.ooici.eoi.netcdf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

/**
 *
 * @author cmueller
 */
public class NcDumpParse {

    private NcDumpParse(){}


    public static String parseToDelimited(String cdl) throws IOException {
        return parseToDelimited(cdl, ",");
    }

    public static String parseToDelimited(String cdl, String delim) throws IOException {
        BufferedReader br = new BufferedReader(new StringReader(cdl));

        StringBuilder headers = new StringBuilder();
        StringBuilder values = new StringBuilder();

        String line;
        while((line = br.readLine()) != null) {
            if(line.startsWith(" :")) {
                headers.append(line.substring(line.indexOf(":") + 1, line.indexOf(" = "))).append(delim);
                values.append(line.substring(line.indexOf(" = ") + 3, line.indexOf(";"))).append(delim);
            }
        }

        return new StringBuilder().append(headers.toString()).append("\n").append(values.toString()).toString();
    }
}
