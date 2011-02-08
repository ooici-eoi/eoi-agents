/*
 * File Name:  AbstractNcAgent.java
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
public abstract class AbstractNcAgent extends AbstractDatasetAgent implements INcAgent {

    /* (non-Javadoc)
     * @see net.ooici.agent.abstraction.AbstractDatasetAgent#processDataset(java.lang.Object)
     */
    @Override
    protected final String[] _processDataset(Object data) {
        if (!(data instanceof NetcdfDataset)) {
            throw new IllegalArgumentException(new StringBuilder("Supplied data must an instance of ")
                                               .append(NetcdfDataset.class.getName())
                                               .append("; '")
                                               .append(data.getClass().getName())
                                               .append("' type was received").toString());
        }
        return processDataset((NetcdfDataset) data);
    }
}
