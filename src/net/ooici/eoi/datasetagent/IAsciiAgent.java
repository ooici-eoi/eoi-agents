/*
 * File Name:  IAsciiAgent.java
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
public interface IAsciiAgent extends IDatasetAgent {

    NetcdfDataset buildDataset(String asciiData);
    
}
