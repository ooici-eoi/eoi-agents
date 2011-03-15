/*
 * File Name:  IDatasetAgent.java
 * Created on: Dec 17, 2010
 */
package net.ooici.eoi.datasetagent;

/**
 * TODO Add class comments
 *
 * @author cmueller
 * @author tlarocque
 * @version 1.0
 */
public interface IDatasetAgent {

    /**
     * This method is called to generate the request that should be used when retrieving data.
     * <p>
     * Refer to documentation for more details.
     * @param context
     * @return
     */
    String buildRequest(net.ooici.services.sa.DataSource.EoiDataContext context);
    
    Object acquireData(String request);
    
    String[] doUpdate(net.ooici.services.sa.DataSource.EoiDataContext context, java.util.HashMap<String, String> connectionInfo);

    void setTesting(boolean isTest);

    void setMaxSize(long maxSize);

    void setDecompDivisor(int decompDivisor);
}
