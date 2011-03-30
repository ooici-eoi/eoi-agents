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

    /**
     * Processes the given <code>data</code> as from {@link #acquireData(String)}. The argument <code>data</code> is assumed to be in
     * instance of a <code>NetcdfDataset</code>. If this is not the case an <code>IllegalArgumentException</code> will be thrown.<br />
     * <br />
     * DatasetDataset processing is delegated to subclasses implementation of {@link #processDataset(NetcdfDataset)}
     * 
     * @param data
     *            a <code>NetcdfDataset</code> result from {@link #acquireData(String)}
     * 
     * @return TODO:
     * 
     * @throws IllegalArgumentException
     *             When the given <code>data</code> is not an instance of <code>NetcdfDataset</code>
     * 
     * @see NetcdfDataset
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
