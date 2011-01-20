/*
 * File Name:  IDatasetAgent.java
 * Created on: Dec 17, 2010
 */
package net.ooici.eoi.datasetagent;

import java.util.Map;

import ucar.nc2.dataset.NetcdfDataset;

/**
 * TODO Add class comments
 * 
 * @author tlarocque
 * @version 1.0
 */
public interface IDatasetAgent {

    String buildRequest(Map<String, String[]> context);
    
    Object acquireData(String request);
    
    NetcdfDataset doUpdate(Map<String, String[]> context);
    
}
