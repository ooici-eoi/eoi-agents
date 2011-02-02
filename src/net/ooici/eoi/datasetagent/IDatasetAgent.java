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

    String buildRequest(net.ooici.services.sa.DataSource.EoiDataContext context);
    
    Object acquireData(String request);
    
    String[] doUpdate(net.ooici.services.sa.DataSource.EoiDataContext context, java.util.HashMap<String, String> connectionInfo);

    void setTesting(boolean isTest);
}
