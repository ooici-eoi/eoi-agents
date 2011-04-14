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
import net.ooici.core.message.IonMessage.ResponseCodes;
import net.ooici.eoi.netcdf.AttributeFactory;
import net.ooici.eoi.netcdf.NcUtils;
import net.ooici.eoi.proto.Unidata2Ooi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.nc2.NetcdfFile;
import ucar.nc2.constants.FeatureType;
import ucar.nc2.dataset.NetcdfDataset;


/**
 * The AbstractDatasetAgent provides the core functionallity used in typical implementations of an <code>IDatasetAgent</code>.<br />
 * <br />
 * This class defines the standard implementation for the method {@link #doUpdate(net.ooici.services.sa.DataSource.EoiDataContextMessage, HashMap)}<br />
 * <br />
 * <b><code>doUpdate()</code></b> Performs a dataset update sequence by chaining
 * calls to the methods:
 * <ol>
 * <li>{@link #buildRequest(net.ooici.services.sa.DataSource.EoiDataContextMessage)}</li>
 * <li>{@link #acquireData(String)}</li>
 * <li>{@link #_processDataset(Object)}</li>
 * </ol>
 * 
 * This class also defines the following helper methods for satisfying dataset update requests:
 * <ul>
 * <li>{@link #sendNetcdfDataset(NetcdfDataset, String)}</li>
 * <li>{@link #sendNetcdfDataset(NetcdfDataset, String, boolean)}</li>
 * <li>{@link #decompSendVariable(ucar.nc2.Variable, ucar.ma2.Section, int)}</li>
 * <li>{@link #addOoiciBoundsMetadata(NetcdfDataset)}</li>
 * <li>{@link #sendDatasetMsg(byte[])}</li>
 * <li>{@link #sendDataChunkMsg(byte[])}</li>
 * <li>{@link #sendDataDoneMsg()}</li>
 * </ul>
 * 
 * <b>Implementation Note:</b><br />
 * Concrete classes are required to implement the following methods:
 * <ul>
 * <li>{@link #buildRequest(net.ooici.services.sa.DataSource.EoiDataContextMessage)}</li>
 * <li>{@link #acquireData(String)}</li>
 * <li>{@link #_processDataset(Object)}</li>
 * </ul>
 * 
 * @author cmueller
 * @author tlarocque
 * @version 1.0
 */
public abstract class AbstractDatasetAgent implements IDatasetAgent {

    private static Logger log = LoggerFactory.getLogger(AbstractDatasetAgent.class);

    /**
     * Used to determine what the agent does after processing an update.
     */
    public enum AgentRunType {

        /**
         * The 'default' runtype which sends messages to the ingest service after processing the update
         */
        NORMAL,
        /**
         * Runs the agent in "test mode" - the update is processed normally, but the response is the 'cdl' dump for the dataset.  No messages or data are sent via messages or written to disk
         */
        TEST_NO_WRITE_DATA,
        /**
         * Runs the agent in "test mode" with results written to disk - the update is processed normally, the response is the 'cdl' dump for the dataset, and the dataset is written to disk @ "out/<ds_title>.nc"
         */
        TEST_WRITE_DATA,
    }
    /**
     * This is to allow for testing without sending data messages (ii.e. to test agent implementations) - set to "TEST_NO_WRITE_DATA" or "TEST_WRITE_DATA" to run in "test" mode
     */
    private AgentRunType runType = AgentRunType.NORMAL;
    /**
     * This is the value used to decompose the dataset when sending.
     * This is the maximum <i>total bytes</i> of <b>data</b> that will be sent in one message.
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
//    private String ingest_op = "ingest";
    private final String RECV_DATASET_OP = "recv_dataset";
    private final String RECV_CHUNK_OP = "recv_chunk";
    private final String RECV_DONE_OP = "recv_done";
    private String recieverQueue = null;

    /*
     * (non-Javadoc)
     * @see net.ooici.eoi.datasetagent.IDatasetAgent#setTesting(boolean)
     */
    @Override
    public void setAgentRunType(AgentRunType agentRunType) {
        runType = agentRunType;
    }

    /*
     * (non-Javadoc)
     * @see net.ooici.eoi.datasetagent.IDatasetAgent#setMaxSize(long)
     */
    @Override
    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    /*
     * (non-Javadoc)
     * @see net.ooici.eoi.datasetagent.IDatasetAgent#setDecompDivisor(int)
     */
    @Override
    public void setDecompDivisor(int decompDivisor) {
        this.decompDivisor = decompDivisor;
    }

    /**
     * TODO:
     * @param rng
     */
    public void addSubRange(Range rng) {
        subRanges.put(rng.getName(), rng);
    }

    /**
     * TODO:
     * @param rng
     */
    public void removeSubRange(Range rng) {
        subRanges.remove(rng.getName());
    }

    /**
     * Performs a dataset update sequence per the requirements in the given <code>context</code>. <code>connectionInfo</code> provides the
     * necessary arguments to establish brokered communication with the service which should ingest the product of said update.<br />
     * <br />
     * Typical update sequences occur over the following steps:<br />
     * <ol>
     * <li>Build a data request for the given <code>context</code><br />
     * {@link #buildRequest(net.ooici.services.sa.DataSource.EoiDataContextMessage)}</li>
     * <li>Acquire data from the previously built request as either <code>String</code> data (CSV, TSV, XML etc) or a <code>NetCdfDataset</code>
     * <br />
     * {@link #acquireData(String)}</li>
     * <li>Process and send the data in part or wholesale depending upon a subclasses implementation<br />
     * {@link #_processDataset(Object)} <br />
     * </li>
     * </ol>
     * 
     * @param context
     *            the current or required state of a given dataset providing context for performing updates upon it
     * @param connectionInfo
     *            mapped parameters used in establishing connectivity for sending the results of a dataset update
     * 
     * @return TODO:
     * 
     * @see #initMsgBrokerClient(HashMap)
     */
    @Override
    public final String[] doUpdate(net.ooici.services.sa.DataSource.EoiDataContextMessage context, java.util.HashMap<String, String> connectionInfo) {
        /* NOTE: Template method.  Do not reorder */

        /* If the connectionInfo object is null, assume this is being called from a test */
        if (connectionInfo == null) {
            runType = AgentRunType.TEST_NO_WRITE_DATA;
        }

        if (runType == AgentRunType.NORMAL) {
            initMsgBrokerClient(connectionInfo);
        }

        String request = buildRequest(context);
        Object data = acquireData(request);
        String[] result = _processDataset(data);

        closeMsgBrokerClient();

        return result;
    }

    /**
     * Processes data as from {@link #acquireData(String)} -- which may be defined by a subclasses implementation
     * 
     * @param data
     *            any arbitrary data to be processed typically the result from {@link #acquireData(String)}
     * 
     * @return TODO:
     */
    protected abstract String[] _processDataset(Object data);

    /**
     * Breaks the given <code>NetcdfDataset</code> into manageable chunks and sends them to an ingestion service using the brokered
     * connection established by {@link #initMsgBrokerClient(HashMap)}. Prior to sending, changes to the <code>NetcdfDataset</code> are
     * finalized by appending necessary metadata and applying {@link ucar.nc2.NetcdfFile#finish()}. This method ensures that dataset chunks are never
     * larger in size than then the maximum size, set by {@link #setMaxSize(long)}
     * 
     * @param ncds
     *            a {@link NetcdfDataset} object
     * @param op
     *            TODO: **not used**
     * 
     * @return TODO: unused?
     * 
     * @see #decompSendVariable(ucar.nc2.Variable, ucar.ma2.Section, int)
     * @see #initMsgBrokerClient(HashMap)
     * @see #sendDatasetMsg(byte[])
     * @see #sendDataChunkMsg(byte[])
     * @see #sendDataDoneMsg()
     */
    protected String sendNetcdfDataset(ucar.nc2.dataset.NetcdfDataset ncds, String op) {
        return sendNetcdfDataset(ncds, op, true);
    }

    /**
     * Breaks the given <code>NetcdfDataset</code> into manageable chunks and sends them to an ingestion service using the brokered
     * connection established by {@link #initMsgBrokerClient(HashMap)}. Prior to sending, changes to the <code>NetcdfDataset</code> are
     * finalized by appending necessary metadata and applying {@link NetcdfFile#finish()}. This method ensures that dataset chunks are never
     * larger in size than then the maximum size, set by {@link #setMaxSize(long)}
     * 
     * @param ncds
     *            a {@link NetcdfDataset} object
     * @param op
     *            TODO: **not used**
     * @param includeData
     *            a <code>boolean</code> specifying whether or not to include the data of the given dataset. If <code>false</code> only
     *            header information will be sent
     * 
     * @return TODO: unused?
     * 
     * @see #decompSendVariable(ucar.nc2.Variable, ucar.ma2.Section, int)
     * @see #initMsgBrokerClient(HashMap)
     * @see #sendDatasetMsg(byte[])
     * @see #sendDataChunkMsg(byte[])
     * @see #sendDataDoneMsg()
     * 
     */
    protected String sendNetcdfDataset(ucar.nc2.dataset.NetcdfDataset ncds, String op, boolean includeData) {
        assert ncds != null;

        /* Apply OOICI geospatial-temporal metadata */
//        addOoiciBoundsMetadata(ncds);

        /* "finish" the dataset - applies any changes that have been applied to ensure they appear in the dataset as appropriate */
        ncds.finish();

        String ret = null;
        switch (runType) {
            case TEST_WRITE_DATA:
                try {
                    /* Dump the dataset locally */
                    new java.io.File("out").mkdir();
                    String outname = ncds.findAttValueIgnoreCase(null, "title", "NO-TITLE");
                    outname = (outname.endsWith(".nc")) ? outname : outname + ".nc";
                    outname = outname.replace(":", "_");
                    ucar.nc2.FileWriter.writeToFile(ncds, "out/" + outname);
                } catch (Exception ex) {
                    log.error("Error writing file during testing...", ex);
                }
            case TEST_NO_WRITE_DATA:
                ret = ncds.toString();
                return ret;
        }

        ResponseCodes respCode = ResponseCodes.OK;
        String respBody = "";

        /* Package the dataset */
        /* Build the OOICI Canonical Representation of the dataset and serialize as a byte[] */
        byte[] dataMessageContent;
        try {
            /** Estimate the size of the dataset */
            long estSize = NcUtils.estimateSize(ncds, subRanges);

            if (estSize <= maxSize) {
                /** Send the full dataset */
                /* TODO: zDeal with subRanges when sending full datasets */
                /* TODO: Do we even want this option anymore?!?! Should we ALWAYS send the data "by variable"?? */
                dataMessageContent = Unidata2Ooi.ncdfToByteArray(ncds);
                sendDatasetMsg(dataMessageContent);
            } else {
                /** Send a "shell" of the dataset */
                dataMessageContent = Unidata2Ooi.ncdfToByteArray(ncds, false);
                sendDatasetMsg(dataMessageContent);
                // IonMessage reply = rpcDataMessage(op, dataMessageContent);
                // ret = reply.getContent().toString();

                /** Send the variables in "chunks" */
                for (ucar.nc2.Variable v : ncds.getVariables()) {
                    log.debug("Processing Variable: " + v.getName());
                    try {

                        /* Get the section for the complete variable - with subranges applied */
                        ucar.ma2.Section sec = NcUtils.getSubRangedSection(v, subRanges);

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
//            IonMsg.Builder endMsgBldr = IonMsg.newBuilder();
//            endMsgBldr.setResponseCode(respCode);
//            endMsgBldr.setResponseBody(respBody);
//            GPBWrapper<IonMsg> msgWrap = GPBWrapper.Factory(endMsgBldr.build());
//            log.debug(msgWrap.toString());
            sendDataDoneMsg();
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
//            if (!runType) {//Not necessary - can't get here if runType (see "sendNetcdfDataset")
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
            sendDataChunkMsg(sbldr.build().toByteArray());
//            }
        }
    }

    /**
     * Adds ION-specific global metadata to the given <code>NetcdfDataset</code> to specify the geospatial and temporal bounds of that
     * dataset
     * 
     * @param ncds
     *            a {@link NetcdfDataset} object
     */
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

//  private double mbToMetersPosDown(double mbPressure) {
//      /* From:  http://www.4wx.com/wxcalc/formulas/pressureAltitude.php  */
//      double ft = (1 - (Math.pow(mbPressure / 1013.25, 0.190284))) * 145366.45;
//      /* Convert feet to meters */
//      return -ft * 0.3048;
//  }
    /**
     * Sends the <code>byte[]</code> representation of a dataset to the remote operation specified by {@link #RECV_DATASET_OP} using the
     * currently active <code>MsgBrokerClient</code>. This message send is one way, that is, the message is NOT sent via RPC conventions
     * 
     * @param dataMessageContent
     *            A serialized <code>byte[]</code> representation of a dataset
     * 
     * @see #initMsgBrokerClient(HashMap)
     * @see Unidata2Ooi#ncdfToByteArray(NetcdfDataset)
     * @see Unidata2Ooi#ncdfToByteArray(NetcdfDataset, boolean)
     */
    protected void sendDatasetMsg(byte[] dataMessageContent) {
        IonMessage dataMessage = cl.createMessage(fromName, toName, RECV_DATASET_OP, dataMessageContent);
        dataMessage.getIonHeaders().put("encoding", "ION R1 GPB");
        log.debug(printMessage("@@@--->>> NetcdfDataset dataset (message) to eoi_ingest", dataMessage));
        cl.sendMessage(dataMessage);
        // return cl.consumeMessage(recieverQueue);
    }

    /**
     * Sends the <code>byte[]</code> representation of a dataset "section" to the remote operation specified by {@link #RECV_CHUNK_OP} using
     * the currently active <code>MsgBrokerClient</code>. This message send is one way, that is, the message is NOT sent via RPC conventions
     * 
     * @param dataMessageContent
     *            A serialized <code>byte[]</code> representation of a dataset section
     * 
     * @see #initMsgBrokerClient(HashMap)
     * @see Unidata2Ooi#ncdfToByteArray(NetcdfDataset)
     * @see Unidata2Ooi#ncdfToByteArray(NetcdfDataset, boolean)
     */
    protected void sendDataChunkMsg(byte[] dataMessageContent) {
        IonMessage dataMessage = cl.createMessage(fromName, toName, RECV_CHUNK_OP, dataMessageContent);
        dataMessage.getIonHeaders().put("encoding", "ION R1 GPB");
        log.debug(printMessage("@@@--->>> NetcdfDataset data chunk (message) to eoi_ingest", dataMessage));
        cl.sendMessage(dataMessage);
    }

    /**
     * Signals the service ingesting dataset data that the most recent stream of "sends" is complete by sending notification to the remote
     * operation specified by {@link #RECV_DONE_OP} using the currently active <code>MsgBrokerClient</code>. This message send is one way,
     * that is, the message is NOT sent via RPC conventions
     * 
     * @see #initMsgBrokerClient(HashMap)
     * @see Unidata2Ooi#ncdfToByteArray(NetcdfDataset)
     * @see Unidata2Ooi#ncdfToByteArray(NetcdfDataset, boolean)
     */
    protected void sendDataDoneMsg() {
        /* Build an container with an empty structure */
        /* TODO: find a better way to invoke this RPC -- the RPC requires no data.. */
        net.ooici.services.dm.IngestionService.SupplementMessage.Builder supMsgBldr = net.ooici.services.dm.IngestionService.SupplementMessage.newBuilder();
        GPBWrapper<net.ooici.services.dm.IngestionService.SupplementMessage> supWrap = GPBWrapper.Factory(supMsgBldr.build());
        net.ooici.core.container.Container.Structure.Builder sbldr = net.ooici.core.container.Container.Structure.newBuilder();
        ProtoUtils.addStructureElementToStructureBuilder(sbldr, supWrap.getStructureElement(), true);

        IonMessage dataMessage = cl.createMessage(fromName, toName, RECV_DONE_OP, sbldr.build().toByteArray());
        dataMessage.getIonHeaders().put("encoding", "ION R1 GPB");
        log.debug(printMessage("@@@--->>> NetcdfDataset data \"DONE\" message to eoi_ingest", dataMessage));
        cl.sendMessage(dataMessage);
    }

    private void initMsgBrokerClient(HashMap<String, String> connectionInfo) {
        toName = new ion.core.messaging.MessagingName(connectionInfo.get("ingest_topic"));
        cl = new ion.core.messaging.MsgBrokerClient(connectionInfo.get("host"), com.rabbitmq.client.AMQP.PROTOCOL.PORT, connectionInfo.get("xp_name"));
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
//            if (!runType) {
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
