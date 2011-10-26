package net.ooici.eoi.datasetagent;

import ion.core.utils.GPBWrapper;
import ion.core.utils.IonUtils;
import ion.core.utils.ProtoUtils;
import ion.core.utils.StructureManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.ooici.core.container.Container;
import net.ooici.core.message.IonMessage;
import net.ooici.core.message.IonMessage.IonMsg;
import net.ooici.services.sa.DataSource;
import net.ooici.services.sa.DataSource.EoiDataContextMessage;

/**
 * This class provides the launching point for performing a dataset update via the EOI Java Dataset Agents
 * @author cmueller
 */
public class JavaAgentLauncher {

    private final static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(JavaAgentLauncher.class);
    private static String host_name;
    private static String exchange_name;

    /**
     * Entry point for the class.  This should be called by a "controlling" process (the java_agent_wrapper.py class in R1) that has the ability to terminate the java process on a system level error (such as an operation timeout).
     * <p>
     * This method requires 2 arguments:1 - the AMQP broker-host; 2 - the messaging exchange-point.  After startup, this class will listen to the Standard In channel (<code>System.in</code>) to receive further information.<br>
     * The information received on Standard In should be a <code>String</code> and can be one of two things:<br>
     * <ol>
     * <li>A JSON representation of a well-formed EoiDataContextMessage ==> This will be parsed and, if well-formed, result in an update being performed</li>
     * <li>The string "INIT_TEST" ==> This will print a "test success" message and exit with a 0 code</li>
     * </ol>
     * 
     * When a JSON EoiDataContextMessage is received, the following methods are called in this order:<br>
     * <ol>
     * <li>{@link #buildContext()}</li>
     * <li>{@link #getDataResourceCreateRequestStructure(java.lang.String)}</li>
     * <li>{@link #performUpdate(Container.Structure)}</li>
     * </ol>
     * @param args 2 required: 1 - the AMQP broker-host; 2 - the messaging exchange-point
     */
    public static void main(String[] args) {
        if (args.length == 2) {
            host_name = args[0];
            exchange_name = args[1];
            if (log.isDebugEnabled()) {
                log.debug("host=={} exchange=={}", host_name, exchange_name);
            }
            Container.Structure struct = null;
            try {
                String context = buildContext();
                struct = getDataResourceCreateRequestStructure(context);
                if (log.isDebugEnabled()) {
                    log.debug("RETURNED_STRUCTURE:\n{}", struct);
                }
            } catch (IOException ex) {
                log.error("Encountered an error building the context object - cannot continue", ex);
                System.exit(1);
            } catch (Exception ex) {
                log.error("Encountered an error building the context object - cannot continue", ex);
                System.exit(1);
            }
            if (struct != null) {
                performUpdate(struct);
                System.exit(0);
            } else {
                log.error("The structure object is null - cannot continue");
                System.exit(1);
            }
        } else {
            log.error("There must be 2 arguments: host, exchange");
            System.exit(1);
        }
    }

    /**
     * This method listens to Standard In (<code>System.in</code>) and parses the received input into a <code>StringBuilder</code>.<br>
     * The content is checked to see if it starts with "INIT_TEST" and if so, exits with a 0 code.  Otherwise, the method returns the content provided on Standard In.
     * @return the content provided on Standard In
     * @throws IOException if an exception is encountered while parsing the input from <code>System.in</code>
     */
    private static String buildContext() throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String line;
        String nl = System.getProperty("line.separator");
        while ((line = br.readLine()) != null && !line.isEmpty()) {
            sb.append(line).append(nl);
        }
        /* Check for test condition */
        if (sb.toString().startsWith("INIT_TEST")) {
            if (log.isInfoEnabled()) {
                log.info("Initialization test successfull on the Java side");
            }
            System.exit(0);
        }

        String ret = sb.toString();
        if (log.isDebugEnabled()) {
            log.debug("CONTEXT_STRING:\n{}", ret);
        }

        return ret;
    }

    /**
     * Takes the <code>String</code> representation of the input captured from Standard In and attempts to generate a valid <code>EoiDataContextMessage</code>.  If this object can be generated, it is bundled into a <code>Container.Structure</code> message which is then returned.
     * @param content The <code>String</code> representation of an <code>EoiDataContextMessage</code>
     * @return A <code>Container.Structure</code> message containing the <code>EoiDataContextMessage</code> and any supporting messages (i.e. <code>DataSource.SearchPattern</code>)
     * @throws IOException If an error is encountered while parsing the <i>content</i>
     * @throws Exception If an error is encountered while building the GPB message(s) from the JSON representation(s)
     */
    private static Container.Structure getDataResourceCreateRequestStructure(String content) throws IOException, Exception {

        Container.Structure struct = null;
        GPBWrapper<DataSource.ThreddsAuthentication> tdsWrap = null;
        GPBWrapper<DataSource.SearchPattern> srchWrap = null;
        List<GPBWrapper<DataSource.SubRange>> subRngList = new ArrayList<GPBWrapper<DataSource.SubRange>>();
        DataSource.EoiDataContextMessage.Builder contextBldr = null;

        Pattern p = Pattern.compile("(?m)#\\s*[a-zA-Z]+?:([0-9]+)\\s*(\\{[^{}]+?\\})");
        Matcher m = p.matcher(content);
        while (m.find()) {
            int resId = Integer.valueOf(content.substring(m.start(1), m.end(1)));
            String json = content.substring(m.start(2), m.end(2));
            switch (resId) {
                case 4501://DataResourceCreateRequest
                    contextBldr = (DataSource.EoiDataContextMessage.Builder) IonUtils.convertJsonToGPBBuilder(json, resId);
                    break;
                case 4504://ThreddsAuthentication
                    tdsWrap = GPBWrapper.Factory((DataSource.ThreddsAuthentication) IonUtils.convertJsonToGPB(json, resId));
                    break;
                case 4505://SearchPattern
                    srchWrap = GPBWrapper.Factory((DataSource.SearchPattern) IonUtils.convertJsonToGPB(json, resId));
                    break;
                case 4506://SubRange - can be repeated
                    subRngList.add(GPBWrapper.Factory((DataSource.SubRange) IonUtils.convertJsonToGPB(json, resId)));
                    break;
            }
        }
        if (contextBldr != null) {
            Container.Structure.Builder sbldr = Container.Structure.newBuilder();
            if (tdsWrap != null) {
                contextBldr.setAuthentication(tdsWrap.getCASRef());
                ProtoUtils.addStructureElementToStructureBuilder(sbldr, tdsWrap.getStructureElement());
            }
            if (srchWrap != null) {
                contextBldr.setSearchPattern(srchWrap.getCASRef());
                ProtoUtils.addStructureElementToStructureBuilder(sbldr, srchWrap.getStructureElement());
            }
            if (!subRngList.isEmpty()) {
                for (GPBWrapper<DataSource.SubRange> rng : subRngList) {
                    contextBldr.addSubRanges(rng.getObjectValue());
                }
            }
            
            /* Check the end time - if negative or < start time, set to now */
            long end = contextBldr.getEndDatetimeMillis();
            long start = contextBldr.getStartDatetimeMillis();
            if(end < 0 || end < start) {
                end = AgentUtils.createUtcCal(new java.util.Date()).getTimeInMillis();
                contextBldr.setEndDatetimeMillis(end);
            }

            GPBWrapper<DataSource.EoiDataContextMessage> contWrap = GPBWrapper.Factory(contextBldr.build());
            if (log.isDebugEnabled()) {
                log.debug("CONTEXT_WRAPPER:\n{}", contWrap.toString());
            }
            ProtoUtils.addStructureElementToStructureBuilder(sbldr, contWrap.getStructureElement());

            IonMessage.IonMsg ionMsg = IonMessage.IonMsg.newBuilder().setIdentity(UUID.randomUUID().toString()).setMessageObject(contWrap.getCASRef()).build();
            ProtoUtils.addStructureElementToStructureBuilder(sbldr, GPBWrapper.Factory(ionMsg).getStructureElement(), true);

            struct = sbldr.build();
        }

        return struct;
    }

    /**
     * Performs the dataset update operation.<br>
     * This method uses the provided <i>struct</i> to obtain an instance of the appropriate <code>IDatasetAgent</code> and then launches the update process by calling {@link IDatasetAgent#doUpdate(net.ooici.core.container.Container.Structure, java.util.HashMap) }
     * @param struct the <code>Container.Structure</code> containing a valid <code>EoiDataContextMessage</code> object
     */
    private static void performUpdate(Container.Structure struct) {
        StructureManager sm = StructureManager.Factory(struct);
        /* Store the EoiDataContext object */
        IonMsg msg = (IonMsg) sm.getObjectWrapper(sm.getHeadId()).getObjectValue();
        EoiDataContextMessage context = (EoiDataContextMessage) sm.getObjectWrapper(msg.getMessageObject()).getObjectValue();
        IDatasetAgent agent;
        agent = AgentFactory.getDatasetAgent(context.getSourceType());
        if (log.isInfoEnabled()) {
            log.info("Agent class ==> {}", agent.getClass().toString());
        }
        java.util.HashMap<String, String> connInfo = new java.util.HashMap<String, String>();
        connInfo.put("ion.host", host_name);
        connInfo.put("ion.exchange", context.getXpName());
        connInfo.put("ion.ingest_topic", context.getIngestTopic());
        if (log.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder("Connection Info:\n");
            for (Entry<String, String> e : connInfo.entrySet()) {
                sb.append("    ").append(e.getKey()).append(" : ").append(e.getValue()).append("\n");
            }
            log.debug(sb.toString());
        }

        if (log.isInfoEnabled()) {
            log.info("**** Perform Update ****");
        }
        agent.setAgentRunType(AbstractDatasetAgent.AgentRunType.NORMAL);
        agent.doUpdate(struct, connInfo);
    }
}
