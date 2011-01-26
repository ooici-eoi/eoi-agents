/*
 * File Name:  AbstractNcAgent.java
 * Created on: Dec 17, 2010
 */
package net.ooici.eoi.datasetagent;

import java.io.FileNotFoundException;
import java.io.IOException;

import net.ooici.eoi.datasetagent.NcdsFactory.NcdsTemplate;
import ucar.nc2.dataset.NetcdfDataset;

/**
 * TODO Add class comments
 * 
 * @author tlarocque
 * @version 1.0
 */
public abstract class AbstractNcAgent extends AbstractDatasetAgent implements INcAgent {

    /* (non-Javadoc)
     * @see net.ooici.agent.abstraction.AbstractDatasetAgent#buildDataset(java.lang.Object)
     */
    @Override
    protected final NetcdfDataset buildDataset(Object data) {
        if (!(data instanceof NetcdfDataset)) {
            throw new IllegalArgumentException(new StringBuilder("Supplied data must an instance of ")
                                               .append(NetcdfDataset.class.getName())
                                               .append("; '")
                                               .append(data.getClass().getName())
                                               .append("' type was received").toString());
        }
        return buildDataset((NetcdfDataset) data);
    }
    
//    /* (non-Javadoc)
//     * @see net.ooici.agent.abstraction.INcAgent#buildDataset(java.lang.String)
//     */
//    @Override
//    public final NetcdfDataset buildDataset(NetcdfDataset ncDataset) {
//        /* NOTE: Template method.  Do not reorder */
//        NcdsTemplate templateType = getNcdsTemplate();
//
//        /* FIXME: Handle this exception correctly or bubble it up (I'd prefer the latter) [TPL] */
//        NetcdfDataset templateDataset = null;
//        try {
//            templateDataset = templateType.getTempDataset();
//        } catch (FileNotFoundException e) {
//            throw new RuntimeException(e);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//
//        return fillNcdsTemplate(templateDataset, ncDataset);
//    }
//
//    protected abstract NcdsTemplate getNcdsTemplate();
//
//    protected abstract NetcdfDataset fillNcdsTemplate(NetcdfDataset template, NetcdfDataset ncDataset);
}
