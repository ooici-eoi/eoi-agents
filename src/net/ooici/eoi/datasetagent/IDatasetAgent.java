/*
 * File Name:  IDatasetAgent.java
 * Created on: Dec 17, 2010
 */
package net.ooici.eoi.datasetagent;


import ucar.nc2.dataset.NetcdfDataset;

/**
 * TODO Add class comments
 * 
 * @author tlarocque
 * @version 1.0
 */
public interface IDatasetAgent {

    String buildRequest(net.ooici.services.sa.DataSource.EoiDataContext context);
    
    Object acquireData(String request);
    
    NetcdfDataset doUpdate(net.ooici.services.sa.DataSource.EoiDataContext context);
    
}
