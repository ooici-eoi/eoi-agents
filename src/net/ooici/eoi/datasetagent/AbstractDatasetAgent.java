/*
 * File Name:  AbstractDatasetAgent.java
 * Created on: Dec 17, 2010
 */
package net.ooici.eoi.datasetagent;

import ion.core.messaging.IonMessage;
import java.io.IOException;
import java.util.HashMap;
import net.ooici.eoi.proto.Unidata2Ooi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * This is to allow for testing without sending data messages (i.e. to test agent implementations) - turn off to bypass messaging
     */
    private boolean testing = false;
    private ion.core.messaging.MsgBrokerClient cl = null;
    private ion.core.messaging.MessagingName toName = null;
    private ion.core.messaging.MessagingName fromName = null;
    private String recieverQueue = null;

    public void setTesting(boolean isTest) {
        testing = isTest;
    }

    /* (non-Javadoc)
     * @see net.ooici.agent.abstraction.IDatasetAgent#doUpdate()
     */
    @Override
    public final String[] doUpdate(net.ooici.services.sa.DataSource.EoiDataContext context, java.util.HashMap<String, String> connectionInfo) {
        /* NOTE: Template method.  Do not reorder */
        
        /* If the connectionInfo object is null, assume this is being called from a test */
        if(connectionInfo == null) {
            testing = true;
        }

        if (!testing) {
            initMsgBrokerClient(connectionInfo);
        }
//        NetcdfDataset dataset = null;

        String request = buildRequest(context);
        Object data = acquireData(request);
//        dataset = processDataset(data);
//        return dataset;
        String[] result = processDataset(data);

        closeMsgBrokerClient();

        return result;
    }

    protected abstract String[] processDataset(Object data);

    protected String sendNetcdfDataset(ucar.nc2.dataset.NetcdfDataset ncds, String op) {
        assert ncds != null;

        String ret = null;
        /* Package the dataset */
        /* Build the OOICI Canonical Representation of the dataset and serialize as a byte[] */
        byte[] dataMessageContent;
        try {
            dataMessageContent = Unidata2Ooi.ncdfToByteArray(ncds);

            if (!testing) {
                IonMessage dataMessage = cl.createMessage(fromName, toName, op, dataMessageContent);
                dataMessage.getIonHeaders().put("encoding", "ION R1 GPB");
                log.debug(printMessage("**NetcdfDataset Message to eoi_ingest**", dataMessage));
                cl.sendMessage(dataMessage);
                IonMessage reply = cl.consumeMessage(recieverQueue);
                ret = reply.getContent().toString();
            } else {
                ret = ncds.toString();
            }
        } catch (IOException ex) {
            log.error(ret = "Error converting NetcdfDataset to OOICI CDM::******\n" + ncds.toString() + "\n******");
            dataMessageContent = null;
        }
        return ret;
    }

    protected String sendNetcdfVariable(ucar.nc2.Variable var, String op) {
        assert var != null;

        String ret = null;
        /* Package the dataset */
        /* Build the OOICI Canonical Representation of the dataset and serialize as a byte[] */
        byte[] dataMessageContent;
        try {
            if (!testing) {
                dataMessageContent = Unidata2Ooi.varToByteArray(var);
                IonMessage dataMessage = cl.createMessage(fromName, toName, op, dataMessageContent);
                dataMessage.getIonHeaders().put("encoding", "ION R1 GPB");
                log.debug(printMessage("**Variable Message to eoi_ingest**", dataMessage));
                cl.sendMessage(dataMessage);
                IonMessage reply = cl.consumeMessage(recieverQueue);
                ret = reply.getContent().toString();
            } else {
                ret = var.toString();
            }
        } catch (IOException ex) {
            log.error("Error converting NetcdfDataset to OOICI CDM");
            dataMessageContent = null;
        }

        return ret;
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
