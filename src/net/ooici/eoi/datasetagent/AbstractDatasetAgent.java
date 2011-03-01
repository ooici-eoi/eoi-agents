/*
 * File Name:  AbstractDatasetAgent.java
 * Created on: Dec 17, 2010
 */
package net.ooici.eoi.datasetagent;

import ion.core.messaging.IonMessage;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import net.ooici.eoi.netcdf.NcUtils;
import net.ooici.eoi.proto.Unidata2Ooi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;

/**
 * TODO Add class comments
 *
 * @author cmueller
 * @author tlarocque
 * @version 1.0
 */
public abstract class AbstractDatasetAgent implements IDatasetAgent {

    private static Logger log = LoggerFactory.getLogger(AbstractDatasetAgent.class);
    /**
     * This is to allow for testing without sending data messages (ii.e. to test agent implementations) - turn off to bypass messaging
     */
    private boolean testing = false;
    /**
     * This is the value used to decompose the dataset when sending.
     * This is the maximum <i>total bytes</i> of <b>data</b> that will be sent indent one message.
     * It does not include wrapper and message size.
     */
    private long maxSize = 5242880;//Default is 5 MB
    /**
     * HashMap of Ranges (keyed by name) that will be applied to the appropriate dimensions
     */
    private HashMap<String, Range> subRanges = new HashMap<String, Range>();
    private ion.core.messaging.MsgBrokerClient cl = null;
    private ion.core.messaging.MessagingName toName = null;
    private ion.core.messaging.MessagingName fromName = null;
    private String recieverQueue = null;

    @Override
    public void setTesting(boolean isTest) {
        testing = isTest;
    }

    @Override
    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    public void addSubRange(Range rng) {
        subRanges.put(rng.getName(), rng);
    }

    public void removeSubRange(Range rng) {
        subRanges.remove(rng.getName());
    }

    /* (non-Javadoc)
     * @see net.ooici.agent.abstraction.IDatasetAgent#doUpdate()
     */
    @Override
    public final String[] doUpdate(net.ooici.services.sa.DataSource.EoiDataContext context, java.util.HashMap<String, String> connectionInfo) {
        /* NOTE: Template method.  Do not reorder */

        /* If the connectionInfo object is null, assume this is being called from a test */
        if (connectionInfo == null) {
            testing = true;
        }

        if (!testing) {
            initMsgBrokerClient(connectionInfo);
        }

        String request = buildRequest(context);
        Object data = acquireData(request);
        String[] result = _processDataset(data);

        closeMsgBrokerClient();

        return result;
    }

    protected abstract String[] _processDataset(Object data);

    protected String sendNetcdfDataset(ucar.nc2.dataset.NetcdfDataset ncds, String op) {
        return sendNetcdfDataset(ncds, op, true);
    }

    protected String sendNetcdfDataset(ucar.nc2.dataset.NetcdfDataset ncds, String op, boolean includeData) {
        assert ncds != null;

        String ret = null;
        if (testing) {
            ret = ncds.toString();
            return ret;
        }
        /* Package the dataset */
        /* Build the OOICI Canonical Representation of the dataset and serialize as a byte[] */
        byte[] dataMessageContent;
        try {
            long estSize = NcUtils.estimateSize(ncds);
            if (estSize <= maxSize) {
                /* Send the full dataset */
                /* TODO: Deal with subRanges when sending full datasets */
                /* TODO: Do we even want this option anymore?!?! Should we ALWAYS send the data "by variable"?? */
                dataMessageContent = Unidata2Ooi.ncdfToByteArray(ncds);
                IonMessage reply = rpcDataMessage(op, dataMessageContent);
                ret = reply.getContent().toString();
            } else {

                /* Send the "empty" dataset */
                dataMessageContent = Unidata2Ooi.ncdfToByteArray(ncds, false);
                IonMessage reply = rpcDataMessage(op, dataMessageContent);
                ret = reply.getContent().toString();

                /* Send the variables */
                for (ucar.nc2.Variable v : ncds.getVariables()) {
                    log.debug("Processing Variable: " + v.getName());
                    try {

                        /* Get the section for the complete variable - must create a new instance because the one in the Variable has 'isImmutable==true' */
                        ucar.ma2.Section sec = new ucar.ma2.Section(v.getShapeAsSection());

                        for (int i = 0; i < sec.getRanges().size(); i++) {
                            Range r = sec.getRange(i);
                            if (subRanges.containsKey(r.getName())) {
                                sec.replaceRange(i, subRanges.get(r.getName()));
                            }
                        }

                        decompSendVariable(v, sec, 0);
                    } catch (Exception ex) {
                        log.error("Error", ex);
                    }
                }
            }

        } catch (IOException ex) {
            log.error(ret = "Error converting NetcdfDataset to OOICI CDM::******\n" + ncds.toString() + "\n******");
            dataMessageContent = null;
        }
        return ret;


    }

    /**
     * Recursive method for decomposing the data content of a variable for sending to OOI in manageable pieces.
     * <p>
     * The variable {@code var} is traversed by dimension (from outermost to innermost) using the class-level field, {@code maxSize} to determine upper-most size limit.
     * As each dimension is encountered, it is set to a length of 1 and the data size for the remaining dimensions is checked.
     * If the remaining data is still to big, another recursion is invoked to traverse the next dimension.
     * This is done until the remaining data is less than {@code maxSize}, or no more inner-dimensions remain (see text after example).
     * <p>
     * Example: assume a variable with dimensions {@code [5, 10, 10]}, that each of the 500 elements is 1 byte, and that the maximum size is 100 bytes.<br>
     * <pre>
     *
     * **first recursion**
     *  section is [5, 10, 10]
     *  if(section-size > maxsize) {
     *      set section to [1, 10, 10]
     *      recurse with new section
     *  } else {
     *      deal with data
     *  }
     *
     * **second recursion**
     *  section is [1, 10, 10]
     *  if(section-size > maxsize) {
     *      set section to [1, 1, 10]
     *      recurse with new section
     *  } else {
     *      deal with data
     *  }
     * </pre>
     * If the final dimension of the variable is reached and the data for the entire dimension is still > {@code maxSize}, the dimension is repeatedly halved until the section is small enough to allow each section of the dimension to be sent sequentially.
     * <p>
     * Continuing the first example:
     * <pre>
     *  section is [1, 1, 10]
     *  while(section_size > maxsize) {
     *      calculate end = section_length / 2
     *      set section to [1:1, 1:1, section_first:end] //half of total
     *      recurse with new section
     *      set section to [1:1, 1:1, end+1:section_last] //this enters back in the top of the while loop
     *  }
     * </pre>
     *
     *
     * @param var the variable to decompose and send
     * @param sec the section describing the current decomposition level
     * @param depth the current decomposition depth
     * @throws InvalidRangeException thrown when one of the ranges within the structure does not match the available data
     */
    private void decompSendVariable(ucar.nc2.Variable var, ucar.ma2.Section sec, int depth) throws InvalidRangeException, IOException {
        /* Setup indenting */
        String indent = "";
        for (int d = 0; d < depth; d++) {
            indent += " ";
        }

        long esize = var.getElementSize();
        long size = sec.computeSize() * esize;
        log.debug(indent + "decomp-depth = " + depth + " :: sec-size = " + size);
        if (size > maxSize) {
            Range rng;
            Range.Iterator iter;
            int i;
            /**
             * If we're at the innermost dimension, so need to split up the dimension
             * Without this we end up sending element-by-element, which is not desirable
             **/
            if (sec.getRank() - 1 == depth) {
                rng = sec.getRange(depth);
                Range inrng;
                while (size > maxSize) {
                    inrng = sec.getRange(depth);
                    size = sec.computeSize() * esize;
                    if (size > maxSize) {
                        int end = inrng.first() + (inrng.length() / 2);
                        end = (end > inrng.last()) ? inrng.last() : end;
                        sec.replaceRange(depth, new Range(rng.getName(), inrng.first(), end));
                        decompSendVariable(var, sec, depth);
                        sec.replaceRange(depth, new Range(rng.getName(), end + 1, inrng.last()));
                    } else {
                        decompSendVariable(var, sec, depth);
                    }
                }
                sec.replaceRange(depth, rng);
            } else {
                rng = sec.getRange(depth);
                iter = rng.getIterator();
                while (iter.hasNext()) {
                    i = iter.next();
                    sec.replaceRange(depth, new Range(rng.getName(), i, i));
                    decompSendVariable(var, sec, depth + 1);
                }
                sec.replaceRange(depth, rng);
            }
        } else {
            if (!testing) {
            log.debug(indent + "--> proc-sec: " + sec.toString());
                /** TODO: Discuss with David to determine how we're sending the data.
                 *  NOTE: The bounded array will always reflect the data that is present in it's relation to the whole section
                 * What's wrapping it?
                 * - the whole variable with just this section worth of data, keyed with the datasetID
                 * - the bounded array, keyed with the datasetID and the variable name (theoretically a unique pair for a given dataset)
                 * - something else??
                 **/

                /* Build the array and the bounded array*/
//            ion.core.utils.GPBWrapper arrWrap = Unidata2Ooi.getOoiArray(var, sec);
//            Cdmvariable.BoundedArray bndArr = Unidata2Ooi.getBoundedArray(sec, (arrWrap != null) ? arrWrap.getCASRef() : null);
//            ion.core.utils.GPBWrapper<Cdmvariable.BoundedArray> baWrap = ion.core.utils.GPBWrapper.Factory(bndArr);
//            log.debug(baWrap.toString());
//            log.debug(arrWrap.toString());
                // <editor-fold defaultstate="collapsed" desc="TODO: Sending of variable ">
//                                                net.ooici.core.container.Container.Structure varStruct = Unidata2Ooi.varToStructure(v, sec);
//                                                log.debug(varStruct);
//                                                dataMessageContent = Unidata2Ooi.varToByteArray(v, sec);
//                                                reply = rpcDataMessage(op, dataMessageContent);
//                                                dataMessageContent = Unidata2Ooi.varToByteArray(v, sec);
//                                                reply = rpcDataMessage(op, dataMessageContent);
//                                                ucar.ma2.Array a = v.read(sec);
//                                                log.debug(a.getSize());
                // </editor-fold>
            }
        }
    }

//    protected String sendNetcdfVariable(String datasetID, ucar.nc2.Variable var, String op) {
//        return sendNetcdfVariable(datasetID, var, var.getShapeAsSection(), op);
//
//
//    }
//
//    protected String sendNetcdfVariable(String datasetID, ucar.nc2.Variable var, ucar.ma2.Section section, String op) {
//        assert var != null;
//
//        String ret = null;
//        /* Package the variable */
//        /* Build the OOICI Canonical Representation of the variable and serialize as a byte[] */
//
//
//        byte[] dataMessageContent;
//
//
//        try {
//            if (!testing) {
//                dataMessageContent = Unidata2Ooi.varToByteArray(var, section);
//                IonMessage reply = rpcDataMessage(op, dataMessageContent);
//                ret = reply.getContent().toString();
//
//
//            } else {
//                ret = var.toString();
//
//
//            }
//        } catch (IOException ex) {
//            log.error("Error converting NetcdfDataset to OOICI CDM");
//            dataMessageContent = null;
//
//
//        }
//
//        return ret;
//
//
//    }
    protected IonMessage rpcDataMessage(String op, byte[] dataMessageContent) {
        IonMessage dataMessage = cl.createMessage(fromName, toName, op, dataMessageContent);
        dataMessage.getIonHeaders().put("encoding", "ION R1 GPB");
        log.debug(printMessage("**NetcdfDataset Message to eoi_ingest**", dataMessage));
        cl.sendMessage(dataMessage);
        return cl.consumeMessage(recieverQueue);
    }

    private void initMsgBrokerClient(HashMap<String, String> connectionInfo) {
        toName = new ion.core.messaging.MessagingName(connectionInfo.get("exchange"), connectionInfo.get("service"));
        cl = new ion.core.messaging.MsgBrokerClient(connectionInfo.get("server"), com.rabbitmq.client.AMQP.PROTOCOL.PORT, connectionInfo.get("topic"));
        fromName = ion.core.messaging.MessagingName.generateUniqueName();
        cl.attach();
        recieverQueue = cl.declareQueue(null);
        cl.bindQueue(recieverQueue, fromName, null);
        cl.attachConsumer(recieverQueue);
    }

    private void closeMsgBrokerClient() {
        if (cl != null) {
            cl.detach();
        }
    }

    /**
     * Generates a multi-line string representation of an {@link IonMessage}
     *
     * @param title a title (description) for this message.  This text will be at the beginning of the multi-line output string.
     * @param msg the {@link IonMessage} to represent as a string.
     * @return a multi-line string representation of the message
     */
    public static String printMessage(String title, IonMessage msg) {
        StringBuilder sb = new StringBuilder("\t\n" + title + "\n");
        sb.append("Headers: ").append("\n");
        java.util.HashMap<String, Object> headers = (java.util.HashMap<String, Object>) msg.getIonHeaders();
        for (String s : headers.keySet()) {
            sb.append("\t").append(s).append(" :: ").append(headers.get(s)).append("\n");
        }
        sb.append("CONTENT: ").append("\n");
        sb.append("\t").append(msg.getContent()).append("\n");
        return sb.toString();
    }
}
