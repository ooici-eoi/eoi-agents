/*
 * File Name:  INcAgent.java
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
public interface INcAgent extends IDatasetAgent {

    String[] processDataset(ucar.nc2.dataset.NetcdfDataset ncds);
    
}
