/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package net.ooici.eoi.netcdf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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
        Map<String, String> map = parseToMap(cdl);

        StringBuilder headers = new StringBuilder();
        StringBuilder values = new StringBuilder();

        for (Entry<String, String> entry : map.entrySet()) {
            headers.append(entry.getKey()).append(delim);
          values.append(entry.getValue()).append(delim);
        }
        
        return new StringBuilder().append(headers.toString()).append("\n").append(values.toString()).toString();
    }

    public static Map<String, String> parseToMap(String cdl) throws IOException {
        Map<String, String> result = new HashMap<String, String>();
        BufferedReader br = new BufferedReader(new StringReader(cdl));
        
        String line;
        String header;
        String value;
        while((line = br.readLine()) != null) {
            if(line.startsWith(" :")) {
                header = line.substring(line.indexOf(":") + 1, line.indexOf(" = "));
                value = line.substring(line.indexOf(" = ") + 3, line.indexOf(";"));
                result.put(header, value);
            }
        }
        
        return result;
    }
}
