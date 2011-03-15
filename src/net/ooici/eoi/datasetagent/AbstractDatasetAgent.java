/*
 * File Name:  AbstractDatasetAgent.java
 * Created on: Dec 17, 2010
 */
package net.ooici.eoi.datasetagent;

import ion.core.messaging.IonMessage;
import ion.core.utils.GPBWrapper;
import ion.core.utils.ProtoUtils;
import java.io.IOException;
import java.util.HashMap;
import net.ooici.cdm.syntactic.Cdmvariable;
import net.ooici.core.message.IonMessage.IonMsg;
import net.ooici.core.message.IonMessage.ResponseCodes;
import net.ooici.eoi.netcdf.AttributeFactory;
import net.ooici.eoi.netcdf.NcUtils;
import net.ooici.eoi.proto.Unidata2Ooi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.nc2.constants.FeatureType;
import ucar.nc2.dataset.NetcdfDataset;

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
     * This is the value used to divide the dimension length when decomposing within a given dimension.
     */
    private int decompDivisor = 2;//Default is to divide by 2
    /**
     * HashMap of Ranges (keyed by name) that will be applied to the appropriate dimensions
     */
    private HashMap<String, Range> subRanges = new HashMap<String, Range>();
    private ion.core.messaging.MsgBrokerClient cl = null;
    private ion.core.messaging.MessagingName toName = null;
    private ion.core.messaging.MessagingName fromName = null;
    private String ingest_op = "ingest";
    private String recieverQueue = null;

    @Override
    public void setTesting(boolean isTest) {
        testing = isTest;
    }

    @Override
    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public void setDecompDivisor(int decompDivisor) {
        this.decompDivisor = decompDivisor;
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
    public final String[] doUpdate(net.ooici.services.sa.DataSource.EoiDataContextMessage context, java.util.HashMap<String, String> connectionInfo) {
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

        /* Apply OOICI geospatial-temporal metadata */
        addOoiciBoundsMetadata(ncds);

        String ret = null;
        if (testing) {
            ret = ncds.toString();
            return ret;
        }

        ResponseCodes respCode = ResponseCodes.OK;
        String respBody = "";

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
//                sendDataMessage(dataMessageContent);
                IonMessage reply = rpcDataMessage(dataMessageContent);
                ret = reply.getContent().toString();
            } else {

                /* Send the "empty" dataset */
                dataMessageContent = Unidata2Ooi.ncdfToByteArray(ncds, false);
                sendDataMessage(dataMessageContent);
//                IonMessage reply = rpcDataMessage(op, dataMessageContent);
//                ret = reply.getContent().toString();

                /* Send the variables */
                for (ucar.nc2.Variable v : ncds.getVariables()) {
                    log.debug("Processing Variable: " + v.getName());
                    try {

                        /* Get the section for the complete variable - must create a new instance because the one in the Variable has 'isImmutable==true' */
                        ucar.ma2.Section sec = new ucar.ma2.Section(v.getShapeAsSection());

                        /* Apply any subRanges */
                        for (int i = 0; i < sec.getRanges().size(); i++) {
                            Range r = sec.getRange(i);
                            if (subRanges.containsKey(r.getName())) {
                                sec.replaceRange(i, subRanges.get(r.getName()));
                            }
                        }

                        /* Decompose and send variable data */
                        decompSendVariable(v, sec, 0);
                    } catch (Exception ex) {
                        log.error("Error Processing Dataset", ex);
                        respCode = ResponseCodes.RECEIVER_ERROR;
                        respBody = AgentUtils.getStackTraceString(ex);
                    }
                }
            }

        } catch (IOException ex) {
            log.error(ret = "Error converting NetcdfDataset to OOICI CDM::******\n" + ncds.toString() + "\n******");
            respCode = ResponseCodes.RECEIVER_ERROR;
            respBody = AgentUtils.getStackTraceString(ex);
        } finally {
            /* Send Message to signify the end of the ingest */
            IonMsg.Builder endMsgBldr = IonMsg.newBuilder();
            endMsgBldr.setResponseCode(respCode);
            endMsgBldr.setResponseBody(respBody);
            GPBWrapper<IonMsg> msgWrap = GPBWrapper.Factory(endMsgBldr.build());
            log.debug(msgWrap.toString());
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
    protected void decompSendVariable(ucar.nc2.Variable var, ucar.ma2.Section sec, int depth) throws InvalidRangeException, IOException {
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
                        int end = inrng.first() + (inrng.length() / decompDivisor);
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
            log.debug(indent + "--> proc-sec: " + sec.toString());
            if (!testing) {
                /* Build the array and the bounded array */
                ion.core.utils.GPBWrapper arrWrap = Unidata2Ooi.getOoiArray(var, sec);
                Cdmvariable.BoundedArray bndArr = Unidata2Ooi.getBoundedArray(sec, (arrWrap != null) ? arrWrap.getCASRef() : null);
                ion.core.utils.GPBWrapper<Cdmvariable.BoundedArray> baWrap = ion.core.utils.GPBWrapper.Factory(bndArr);
////                log.debug(baWrap.toString());
////                log.debug(arrWrap.toString());

                /* Build the supplement message */
                net.ooici.services.dm.IngestionService.SupplementMessage.Builder supMsgBldr = net.ooici.services.dm.IngestionService.SupplementMessage.newBuilder();
                supMsgBldr.setDatasetId("");
                supMsgBldr.setVariableName(var.getName());
                supMsgBldr.setBoundedArray(baWrap.getCASRef());
                GPBWrapper<net.ooici.services.dm.IngestionService.SupplementMessage> supWrap = GPBWrapper.Factory(supMsgBldr.build());

                /* init the struct bldr */
                net.ooici.core.container.Container.Structure.Builder sbldr = net.ooici.core.container.Container.Structure.newBuilder();
                /* add the supplement wrapper as the head */
                ProtoUtils.addStructureElementToStructureBuilder(sbldr, supWrap.getStructureElement(), true);
                /* add the bounded array and ndarray as items */
                ProtoUtils.addStructureElementToStructureBuilder(sbldr, baWrap.getStructureElement());
                ProtoUtils.addStructureElementToStructureBuilder(sbldr, arrWrap.getStructureElement());

//                sendDataMessage(sbldr.build().toByteArray());
            }
        }
    }

    protected void addOoiciBoundsMetadata(NetcdfDataset ncds) {
        FeatureType ft = NcUtils.determineFeatureType(ncds);

        /* Do Time */
        AttributeFactory.addTimeBoundsMetadata(ncds, subRanges);

        /* Do Lat */
        AttributeFactory.addLatBoundsMetadata(ncds, ft);

        /* Do Lon */
        AttributeFactory.addLonBoundsMetadata(ncds, ft);

        /* Do Vert */
        AttributeFactory.addVertBoundsMetadata(ncds, ft);

    }

//    private double mbToMetersPosDown(double mbPressure) {
//        /* From:  http://www.4wx.com/wxcalc/formulas/pressureAltitude.php  */
//        double ft = (1 - (Math.pow(mbPressure / 1013.25, 0.190284))) * 145366.45;
//        /* Convert feet to meters */
//        return -ft * 0.3048;
//    }

    protected IonMessage rpcDataMessage(byte[] dataMessageContent) {
        IonMessage dataMessage = cl.createMessage(fromName, toName, ingest_op, dataMessageContent);
        dataMessage.getIonHeaders().put("encoding", "ION R1 GPB");
        log.debug(printMessage("**NetcdfDataset Message to eoi_ingest**", dataMessage));
        cl.sendMessage(dataMessage);
        return cl.consumeMessage(recieverQueue);
    }

    protected void sendDataMessage(byte[] dataMessageContent) {
        IonMessage dataMessage = cl.createMessage(fromName, toName, ingest_op, dataMessageContent);
        dataMessage.getIonHeaders().put("encoding", "ION R1 GPB");
        log.debug(printMessage("**NetcdfDataset Message to eoi_ingest**", dataMessage));
        cl.sendMessage(dataMessage);
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
            cl = null;
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
        StringBuilder sb = new StringBuilder("\n" + title + "\n");
        sb.append("Headers: ").append("\n");
        java.util.HashMap<String, Object> headers = (java.util.HashMap<String, Object>) msg.getIonHeaders();
        for (String s : headers.keySet()) {
            sb.append("\t").append(s).append(" :: ").append(headers.get(s)).append("\n");
        }
        sb.append("CONTENT: ").append("\n");
        sb.append("\t").append(msg.getContent()).append("\n");
        return sb.toString();
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
}
