/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.datasetagent;

import ion.core.PollingProcess;
import ion.core.messaging.IonMessage;
import ion.core.messaging.MessagingName;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import net.ooici.eoi.proto.Unidata2Ooi;
import net.ooici.eoi.datasetagent.ControlEvent.ControlEventType;
import net.ooici.eoi.datasetagent.DatasetAgentController.ControlThread.ControlProcess;
import ucar.nc2.dataset.NetcdfDataset;

/**
 *
 * @author cmueller
 */
public class DatasetAgentController implements ControlListener {

    static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DatasetAgentController.class);
    private final ControlThread controlThread;
    private final ExecutorService processService;

    public static void main(String[] args) {
        String host = "";
        String exchange = "";
        String wrapperName = "";
        String bindingCallback = "";
        if (args.length == 4) {
            try {
                host = args[0];
                exchange = args[1];
                wrapperName = args[2];
                bindingCallback = args[3];
            } catch (IllegalArgumentException ex) {
                /* No-Op */
            }

            new DatasetAgentController(host, exchange, wrapperName, bindingCallback);
        }
    }

    public DatasetAgentController(String host, String exchange, String wrapperName, String bindingCallback) {
        controlThread = new ControlThread(host, exchange, null, 500, this);
        log.debug("Control ID: " + controlThread.getMessagingName());

        /* Start the control thread */
        controlThread.start();

        /* Inform the wrapper of my messaging name */
        controlThread.sendControlMessage(wrapperName, bindingCallback, controlThread.getMessagingName().toString());

        /* Instantiate the processing ExecutorService */
        processService = Executors.newFixedThreadPool(1);
    }

    public void controlEvent(ControlEvent evt) {
        switch (evt.getEventType()) {
            case UPDATE:
                /* Perform an update - executes in a seperate thread!! */
                performUpdate(evt.getSource(), evt.getIonMessage());
                break;
            case SHUTDOWN:
                log.debug("Shutting down DatasetAgentController {MessagingID=" + controlThread.getMessagingName() + "}");
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

    public void performUpdate(final Object source, final IonMessage msg) {
        processService.execute(new Runnable() {

            public void run() {
                long threadId = Thread.currentThread().getId();
                String status = "";
                log.debug("Processing Thread ID: " + threadId);
                log.debug(threadId + ":: Received context as: " + msg.getContent().getClass().getName());
                log.debug(threadId + ":: Received context as: " + msg.getContent().getClass().getName());
                Map<String, String[]> context = convertToStringStringArrayMap(((HashMap<?, ?>) msg.getContent()));

                String source_type = context.get("source_type")[0];
                log.debug(threadId + ":: source_type = " + source_type);

                /* Instantiate the appropriate dataset agent based on the source_type */
                IDatasetAgent agent;
                try {
                    agent = AgentFactory.getDatasetAgent(source_type);
                } catch (IllegalArgumentException ex) {
                    IonMessage reply = ((ControlProcess) source).createMessage(msg.getIonHeaders().get("reply-to").toString(), "result", ex.getMessage());
                    reply.getIonHeaders().putAll(msg.getIonHeaders());
                    reply.getIonHeaders().put("receiver", msg.getIonHeaders().get("reply-to").toString());
                    reply.getIonHeaders().put("reply-to", ((ControlProcess)source).getMessagingName().toString());
                    reply.getIonHeaders().put("sender", ((ControlProcess)source).getMessagingName().toString());
                    reply.getIonHeaders().put("status", "ERROR");
                    reply.getIonHeaders().put("conv-seq", Integer.valueOf(msg.getIonHeaders().get("conv-seq").toString()) + 1);
                    reply.getIonHeaders().put("response", "ION ERROR");

                    log.debug(printMessage("**Reply Message to Wrapper**", reply));

                    ((ControlProcess) source).send(reply);
                    return;
                }

                /* Perform the update */
                NetcdfDataset ncds = agent.doUpdate(context);

                /* If the resulting ncds is non-null, upload it */
                if(ncds != null) {
                    /* Build the OOICI Canonical Representation of the dataset and serialize as a byte[] */
                    byte[] dataBytes;
                    try {
                        dataBytes = Unidata2Ooi.ncdfToByteArray(ncds);
                    } catch (IOException ex) {
                        log.error("Error converting NetcdfDataset to OOICI CDM");
                        dataBytes = null;
                    }

                    /* Send the resulting bytes to ION */
                    ion.core.messaging.MsgBrokerClient cl = null;
                    String ooiDsId = null;
                    try {
                        ion.core.messaging.MessagingName toName = new ion.core.messaging.MessagingName("eoitest", "eoi_ingest");
                        cl = new ion.core.messaging.MsgBrokerClient("macpro", com.rabbitmq.client.AMQP.PROTOCOL.PORT, "magnet.topic");
                        ion.core.messaging.MessagingName procId = ion.core.messaging.MessagingName.generateUniqueName();
                        cl.attach();
                        String queue = cl.declareQueue(null);
                        cl.bindQueue(queue, procId, null);
                        cl.attachConsumer(queue);

                        /* Build an IonMessage with the Dataset byte array as content */
//                    controlThread.sendControlMessage((String)msg.getIonHeaders().get("reply-to"), context.get("callback")[0], bytes);
                        log.debug("@@@--->>> Sending NetCDF Dataset byte[] message to " + (String) msg.getIonHeaders().get("reply-to") + "  op: " + context.get("callback")[0]);
//                    IonMessage msgout = ((ControlProcess) source).createMessage((String) msg.getIonHeaders().get("reply-to"), context.get("callback")[0], bytes);
                        ion.core.messaging.IonMessage msgout = cl.createMessage(procId, toName, "ingest", dataBytes);

                        /* Adjust the message headers and send */
                        msgout.getIonHeaders().put("encoding", "ION R1 GPB");

                        log.debug(printMessage("**Data Message to eoi_ingest**", msgout));


                        /* Send data to eoi_ingest service */
                        log.debug("@@@--->>> Sending NetCDF Dataset byte[] message to \"eoitest.eoi_ingest\" op: \"ingest\"");
                        cl.sendMessage(msgout);
                        IonMessage msgin = cl.consumeMessage(queue);
                        log.debug(ooiDsId = msgin.getContent().toString());

                    } finally {
                        if (cl != null) {
                            cl.detach();
                        }
                    }

                    IonMessage reply = ((ControlProcess) source).createMessage(msg.getIonHeaders().get("reply-to").toString(), "result", ooiDsId);
                    reply.getIonHeaders().putAll(msg.getIonHeaders());
                    reply.getIonHeaders().put("receiver", msg.getIonHeaders().get("reply-to").toString());
                    reply.getIonHeaders().put("reply-to", ((ControlProcess)source).getMessagingName().toString());
                    reply.getIonHeaders().put("sender", ((ControlProcess)source).getMessagingName().toString());
                    reply.getIonHeaders().put("status", "OK");
                    reply.getIonHeaders().put("conv-seq", Integer.valueOf(msg.getIonHeaders().get("conv-seq").toString()) + 1);
                    reply.getIonHeaders().put("response", "ION SUCCESS");

                    log.debug(printMessage("**Reply Message to Wrapper**", reply));

                    ((ControlProcess) source).send(reply);
                }

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
            cp.send(new MessagingName(toName), op, content);
        }

        public void terminate() {
            log.debug("ControlThread:: Terminating " + this.getName() + "...");
            interrupt();
        }

        @Override
        public synchronized void start() {
            super.start();
            log.debug("ControlThread:: Starting control thread...");
            if (cp != null) {
                cp.spawn();
            }
        }

        @Override
        public void interrupt() {
            log.debug("ControlThread:: Interrupting " + this.getName() + "...");
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

                log.debug(printMessage("**Control Message Received**", msg));

                String op = msg.getIonHeaders().get("op").toString();
                log.debug("OP: " + op);
                String repTo = msg.getIonHeaders().get("reply-to").toString();
                if (op.equalsIgnoreCase("op_shutdown")) {
                    log.debug("Shutdown Request Received");
//                    this.send(new MessagingName(repTo), "op_shutdown_ack", "Shutdown initiated");
                    clistener.controlEvent(new ControlEvent(this, ControlEventType.SHUTDOWN, msg));
                } else if (op.equalsIgnoreCase("op_update")) {
                    log.debug("Update Request Received");
                    clistener.controlEvent(new ControlEvent(this, ControlEventType.UPDATE, msg));
                }
            }
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
