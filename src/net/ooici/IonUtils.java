/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

/**
 *
 * @author cmueller
 */
public class IonUtils {

    /**
     * Returns a <code>HashMap</code> containing the values from the supplied properties file.
     *
     * @param propertiesFile the properties file to parse
     * @return the map of properties
     * @throws IOException
     */
    public static HashMap<String, String> parseProperties(File propertiesFile) throws IOException {
        Properties propsIn = new Properties();
        propsIn.load(new FileInputStream(propertiesFile));
        HashMap<String, String> props = new HashMap<String, String>();
        for (Iterator<Entry<Object, Object>> iter = propsIn.entrySet().iterator(); iter.hasNext();) {
            Entry<Object, Object> entry = iter.next();
            props.put((String) entry.getKey(), (String) entry.getValue());
        }
        return props;
    }

    /**
     * Defers to {@link parseProperties(File propertiesFile) } using <code>new File(System.getProperty("user.home") + "/ooici-conn.properties")</code> as the <code>propertiesFile</code>
     * @return the map of properties
     * @throws IOException
     */
    public static HashMap<String, String> parseProperties() throws IOException {
        /* Look for connection properties in the user.home directory */
        java.io.File connFile = new java.io.File(System.getProperty("user.home") + "/ooici-conn.properties");

        HashMap<String, String> props = null;
        if (connFile.exists()) {
            props = parseProperties(connFile);
        }
        return props;
    }
}
