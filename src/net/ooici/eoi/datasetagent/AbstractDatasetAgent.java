/*
 * File Name:  AbstractDatasetAgent.java
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
public abstract class AbstractDatasetAgent implements IDatasetAgent {

    /* (non-Javadoc)
     * @see net.ooici.agent.abstraction.IDatasetAgent#doUpdate()
     */
    @Override
    public final NetcdfDataset doUpdate(Map<String, String[]> context) {
        /* NOTE: Template method.  Do not reorder */
        NetcdfDataset dataset = null;
        
        String request = buildRequest(context);
        Object data = acquireData(request);
        dataset = buildDataset(data);
        
        return dataset;
    }
    
    protected abstract NetcdfDataset buildDataset(Object data);
    
}
