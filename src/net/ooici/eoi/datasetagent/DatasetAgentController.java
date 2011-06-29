/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.datasetagent;

import ion.core.IonBootstrap;
import ion.core.PollingProcess;
import ion.core.messaging.IonMessage;
import ion.core.messaging.MessagingName;
import ion.core.utils.StructureManager;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import net.ooici.core.message.IonMessage.IonMsg;
import net.ooici.eoi.datasetagent.ControlEvent.ControlEventType;
import net.ooici.eoi.datasetagent.DatasetAgentController.ControlThread.ControlProcess;
import net.ooici.services.sa.DataSource.EoiDataContextMessage;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

/**
 *
 * @author cmueller
 */
public class DatasetAgentController implements ControlListener {

    static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DatasetAgentController.class);
    private final ControlThread controlThread;
    private final ExecutorService processService;
    private Future<?> runningTask = null;
    private static String w_host_name = "";
    private static String w_exchange_name = "";       /* Wrapper Service Exchange Point Name aka Topic (usu. magnet.topic) */

    private static String w_scoped_name = "";   /* Qualified Wrapper Service Name */


    public static void main(String[] args) {
        String w_callback_op = "";
        if (args.length == 4) {
            try {
                w_host_name = args[0];
                w_exchange_name = args[1];
                w_scoped_name = args[2];
                w_callback_op = args[3];

                if (log.isDebugEnabled()) {
                    log.debug("Startup args: host={}; exchange={}; scoped_name={}; callback={}", new Object[]{w_host_name, w_exchange_name, w_scoped_name, w_callback_op});
                }
            } catch (IllegalArgumentException ex) {
                log.error("Incorrect number of startup args.", ex);
            }

            new DatasetAgentController(w_host_name, w_exchange_name, w_scoped_name, w_callback_op);
        }
    }

    public DatasetAgentController(String host, String exchange, String wrapperName, String bindingCallback) {
        try {
            IonBootstrap.bootstrap();
        } catch (Exception ex) {
            log.error("Error bootstrapping", ex);
        }

        controlThread = new ControlThread(host, exchange, null, 500, this);
        if (log.isDebugEnabled()) {
            log.debug("Control ID: " + controlThread.getMessagingName());
        }

        /* Start the control thread */
        controlThread.start();

        /* Instantiate the processing ExecutorService */
        processService = Executors.newFixedThreadPool(1);

        /* Inform the wrapper of my messaging name */
        String myName = controlThread.getMessagingName().toString();
        if (log.isDebugEnabled()) {
            log.debug("Sending message to wrapper with the DAC Binding Key: {}", myName);
        }
        controlThread.sendControlMessage(wrapperName, bindingCallback, myName);
    }

    @Override
    public void controlEvent(ControlEvent evt) {
        switch (evt.getEventType()) {
            case UPDATE:
                /* Perform an update - executes in a seperate thread!! */
                if (log.isInfoEnabled()) {
                    log.info("****** Perform Update ******");
                }
                performUpdate(evt.getSource(), evt.getIonMessage());
                break;
            case INGEST_ERROR:
                if (log.isInfoEnabled()) {
                    log.info("Ingestion encountered an error - terminating the current processing thread...");
                }
                if (runningTask != null) {
                    if (!runningTask.isDone() && !runningTask.isCancelled()) {
                        if (runningTask.cancel(true)) {
                            if (log.isInfoEnabled()) {
                                log.info("Process Terminated Successfully!");
                            }
                            runningTask = null;
                        } else {
                            if (log.isInfoEnabled()) {
                                log.info("Process Not Terminated");
                            }
                        }
                    }
                }
                break;
            case SHUTDOWN:
                if (log.isInfoEnabled()) {
                    log.info("Shutting down DatasetAgentController:: MessagingID={}", controlThread.getMessagingName());
                }
                boolean terminated = false;
                String terminationStatus = "Termination Successful!!";
                try {
                    processService.shutdown();
                    try {
                        // Wait a while for existing tasks to terminate
                        if (!(terminated = processService.awaitTermination(60, TimeUnit.SECONDS))) {
                            processService.shutdownNow(); // Cancel currently executing tasks
                            // Wait a while for tasks to respond to being cancelled
                            if (!(terminated = processService.awaitTermination(60, TimeUnit.SECONDS))) {
                                /* Failed to terminate!! */
                                terminationStatus = "Failed to Terminate Process!!";
                            }
                        }
                    } catch (InterruptedException ex) {
                        // (Re-)Cancel if current thread also interrupted
                        processService.shutdownNow();
                        // Preserve interrupt status
                        Thread.currentThread().interrupt();
                        terminationStatus = "Control thread interrupted";
                    }
                } finally {
                    if (terminated) {
                        controlThread.terminate();
                    }
                }
                break;
        }
    }

    public void sendNetcdfDataset(NetcdfDataset ncds) {
    }

    public void sendVariable(Variable var) {
    }

    public void performUpdate(final Object source, final IonMessage msg) {
        runningTask = processService.submit(new Runnable() {
//        processService.execute(new Runnable() {

            @Override
            public void run() {
                String threadId = Thread.currentThread().getName();
                String status = "";
                if (log.isDebugEnabled()) {
                    log.debug("Processing Thread ID: " + threadId);
                }
                net.ooici.services.sa.DataSource.EoiDataContextMessage context = null;
                net.ooici.core.container.Container.Structure struct = null;
                try {
                    struct = net.ooici.core.container.Container.Structure.parseFrom((byte[]) msg.getContent());

                    StructureManager sm = StructureManager.Factory(msg);
                    /* Store the EoiDataContext object */
                    IonMsg msg = (IonMsg) sm.getObjectWrapper(sm.getHeadId()).getObjectValue();
                    context = (EoiDataContextMessage) sm.getObjectWrapper(msg.getMessageObject()).getObjectValue();
                    
                    if (log.isDebugEnabled()) {
                        log.debug("ProcThread:" + threadId + ":: Received context as:\n{\n" + context.toString() + "}\n");
                    }

                } catch (Exception ex) {
                    log.error("ProcThread:" + threadId + ":: Received bad context ");
                    IonMessage reply = ((ControlProcess) source).createMessage(context.getIngestTopic(), "result", ex.getMessage());
                    reply.getIonHeaders().put("status", "ERROR");
                    reply.getIonHeaders().put("response", "ION ERROR");
                    reply.getIonHeaders().put("performative", "failure");
                    reply.getIonHeaders().put("encoding", "json");

                    if (log.isDebugEnabled()) {
                        log.debug(reply.toString());
                    }

                    ((ControlProcess) source).send(reply);
                    /* TODO: should we return here? */
                    return;
                }

                net.ooici.services.sa.DataSource.SourceType source_type = context.getSourceType();
                if (log.isDebugEnabled()) {
                    log.debug("ProcThread:" + threadId + ":: source_type = " + source_type);
                }

                /* Instantiate the appropriate dataset agent based on the source_type */
                IDatasetAgent agent;
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("ProcThread:" + threadId + ":: Generating DatasetAgent instance...");
                    }
                    agent = AgentFactory.getDatasetAgent(source_type);
                } catch (IllegalArgumentException ex) {
                    IonMessage reply = ((ControlProcess) source).createMessage(context.getIngestTopic(), "result", ex.getMessage());
                    reply.getIonHeaders().put("status", "ERROR");
                    reply.getIonHeaders().put("response", "ION ERROR");
                    reply.getIonHeaders().put("performative", "failure");
                    reply.getIonHeaders().put("encoding", "json");

                    if (log.isDebugEnabled()) {
                        log.debug(reply.toString());
                    }

                    ((ControlProcess) source).send(reply);
                    return;
                }

                /* Perform the update */
                /* TODO: Make the connection information default to the conn-info for the DAC...will
                 * be replaced by a proto object containing this information.
                 */
                if (log.isDebugEnabled()) {
                    log.debug("ProcThread:" + threadId + ":: Build connInfo");
                }
                java.util.HashMap<String, String> connInfo = new java.util.HashMap<String, String>();
                connInfo.put("ion.host", w_host_name);
                connInfo.put("ion.exchange", context.getXpName());
                connInfo.put("ion.ingest_topic", context.getIngestTopic());

                /*
                 * Perform the update - this can result in multiple messages being sent to the ingest service
                 * The reply should be the ooi resource id
                 */
                if (log.isDebugEnabled()) {
                    log.debug("ProcThread:" + threadId + ":: Perform update");
                }
                String[] ooiDsId = null;
                try {
                    ooiDsId = agent.doUpdate(struct, connInfo);
                } catch (IngestException ex) {
                    return; // Bail out of processing
                } catch (Exception ex) {
                    /* Send a reply_err message back to caller */
                    log.error("ProcThread:" + threadId + ":: Could not perform update", ex);
                    String trace = AgentUtils.getStackTraceString(ex).replaceAll("^", "\t");

                    IonMessage reply = ((ControlProcess) source).createMessage(context.getIngestTopic(), "result", trace);
                    reply.getIonHeaders().put("status", "ERROR");
                    reply.getIonHeaders().put("response", "ION ERROR");
                    reply.getIonHeaders().put("performative", "failure");
                    reply.getIonHeaders().put("encoding", "json");

                    if (log.isDebugEnabled()) {
                        log.debug(reply.toString());
                    }

                    ((ControlProcess) source).send(reply);
                    return;
                }

//                if (log.isDebugEnabled()) {
//                    log.debug("ProcThread:" + threadId + ":: Update complete - send reply to wrapper");
//                }
//                IonMessage reply = ((ControlProcess) source).createMessage(context.getIngestTopic(), "result", ooiDsId[0]);
//                reply.getIonHeaders().put("status", "OK");
//                reply.getIonHeaders().put("response", "ION SUCCESS");
//                reply.getIonHeaders().put("performative", "inform_result");
//                reply.getIonHeaders().put("encoding", "json");
//
////                IonMessage reply = ((ControlProcess) source).createMessage(msg.getIonHeaders().get("reply-to").toString(), "result", ooiDsId[0]);
////                reply.getIonHeaders().putAll(msg.getIonHeaders());
////                reply.getIonHeaders().put("receiver", msg.getIonHeaders().get("reply-to").toString());
////                reply.getIonHeaders().put("reply-to", ((ControlProcess) source).getMessagingName().toString());
////                reply.getIonHeaders().put("sender", ((ControlProcess) source).getMessagingName().toString());
////                reply.getIonHeaders().put("encoding", "json");
////                reply.getIonHeaders().put("status", "OK");
////                reply.getIonHeaders().put("conv-seq", Integer.valueOf(msg.getIonHeaders().get("conv-seq").toString()) + 1);
////                reply.getIonHeaders().put("response", "ION SUCCESS");
////                reply.getIonHeaders().put("performative", "inform_result");
//
//                if (log.isDebugEnabled()) {
//                    log.debug(reply.toString());
//                }
//
//                ((ControlProcess) source).send(reply);
            }
        });
    }

    class ControlThread extends Thread {

        private ControlProcess cp = null;

        public ControlThread(String host, String exchangeTopic, String sysName, ControlListener clistener) {
            this(host, exchangeTopic, sysName, 1000, clistener);
        }

        public ControlThread(String host, String exchangeTopic, String sysName, int pollingInterval, ControlListener clistener) {
            cp = new ControlProcess(host, exchangeTopic, sysName, pollingInterval, clistener);
        }

        public String getQueueName() {
            return cp.getInQueue();
        }

        public MessagingName getMessagingName() {
            return cp.getMessagingName();
        }

        public void sendControlMessage(String toName, String op, Object content) {
            if (log.isDebugEnabled()) {
                log.debug("\n\n\ntoName: \t" + toName + "\nop: \t" + op + "\ncontent: \t" + content);
            }
            cp.send(new MessagingName(toName), op, content);
        }

        public void terminate() {
            if (log.isDebugEnabled()) {
                log.debug("ControlThread:: Terminating " + this.getName() + "...");
            }
            interrupt();
        }

        @Override
        public synchronized void start() {
            super.start();
            if (log.isDebugEnabled()) {
                log.debug("ControlThread:: Starting control thread...");
            }
            if (cp != null) {
                cp.spawn();
            }
        }

        @Override
        public void interrupt() {
            if (log.isDebugEnabled()) {
                log.debug("ControlThread:: Interrupting " + this.getName() + "...");
            }
            cp.dispose();
            super.interrupt();
        }

        class ControlProcess extends PollingProcess {

            private ControlListener clistener;

            public ControlProcess(String host, String exchangeTopic, String sysName, ControlListener clistener) {
                this(host, exchangeTopic, sysName, 1000, clistener);
            }

            public ControlProcess(String host, String exchangeTopic, String sysName, int pollingInterval, ControlListener clistener) {
                super(host, exchangeTopic, sysName, pollingInterval);
                this.clistener = clistener;
            }

            public IonMessage createMessage(String toName, String op, Object content) {
                return cp.mBrokerClient.createMessage(getMessagingName(), new MessagingName(toName), op, content);
            }

            public void send(IonMessage msg) {
                cp.mBrokerClient.sendMessage(msg);
            }

            @Override
            public void messageReceived(ion.core.messaging.IonMessage msg) {
                /* Acknowledge receipt of the message*/
//                log.debug("Ack Message");
                this.ackMessage(msg);

                if (log.isDebugEnabled()) {
                    log.debug(msg.toString());
                }

                String op = msg.getIonHeaders().get("op").toString();
                if (op.equalsIgnoreCase("op_shutdown")) {
                    if (log.isInfoEnabled()) {
                        log.info("Shutdown Request Received");
                    }
//                    this.send(new MessagingName(repTo), "op_shutdown_ack", "Shutdown initiated");
                    clistener.controlEvent(new ControlEvent(this, ControlEventType.SHUTDOWN, msg));
                } else if (op.equalsIgnoreCase("op_update")) {
                    if (log.isInfoEnabled()) {
                        log.info("Update Request Received");
                    }
                    clistener.controlEvent(new ControlEvent(this, ControlEventType.UPDATE, msg));
                } else if (op.equalsIgnoreCase("op_ingest_error")) {
                    if (log.isInfoEnabled()) {
                        log.info("Ingestion Error Received");
                    }
                    clistener.controlEvent(new ControlEvent(this, ControlEventType.INGEST_ERROR, msg));
                } else {
                    if (log.isWarnEnabled()) {
                        log.warn("OP: \"{}\" is not understood, must be either \"op_shutdown\" or \"op_update\"", op);
                    }
                }
            }
        }
    }

    public static Map<String, String[]> convertToStringStringArrayMap(Map<?, ?> params) {
        Map<String, String[]> parameters = new HashMap<String, String[]>();
        Iterator<?> iter = params.keySet().iterator();
        String key = null;
        String[] val = null;
        Object tempVal = null;

        while (iter.hasNext()) {
            key = (String) iter.next();
            tempVal = params.get(key);
            if (tempVal instanceof Object[]) {
                Object[] tempArray = (Object[]) tempVal;
                val = new String[tempArray.length];
                for (int x = 0; x < tempArray.length; x++) {
                    val[x] = (String) tempArray[x];
                }
            } else {
                val = new String[]{(String) tempVal};
            }
            parameters.put(key, val);
        }

        return parameters;
    }
}
