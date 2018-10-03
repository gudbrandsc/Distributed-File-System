import Hash.BalancedHashRing;
import Hash.HashRingEntry;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

/**
 * @author Gudbrand Schistad
 * Thread class that is used to print a message recived from an other user, and send a reply.
 */
public class MessageListener extends Thread {
    private Socket socket;
    private volatile HashMap<String, StorageNodeInfo> storageNodeInfos;
    private int nodeId;
    private volatile HashMap<String, Clientproto.SNReceive> dataStorage;
    private BalancedHashRing balancedHashRing;
    private ArrayList<String> filesInSystem;

    /**
     * Constructor
     */
    MessageListener(Socket socket, HashMap<String, StorageNodeInfo> storageNodeInfos, int nodeId, BalancedHashRing balancedHashRing, HashMap<String, Clientproto.SNReceive> dataStorage, ArrayList<String> filesInSystem) {
        this.socket = socket;
        this.storageNodeInfos = storageNodeInfos;
        this.nodeId = nodeId;
        this.balancedHashRing = balancedHashRing;
        this.dataStorage = dataStorage;
        this.filesInSystem = filesInSystem;
    }

    /**
     * Run method that prints the message and sends a reply
     */
    public void run() {
        Clientproto.SNReceive snReceive = null;

        try {
            snReceive = Clientproto.SNReceive.parseDelimitedFrom(socket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (snReceive != null) {
            if (snReceive.getType() == Clientproto.SNReceive.packetType.STORE) {
                StoreRequestThread storeRequestThread = new StoreRequestThread(socket, snReceive, balancedHashRing, nodeId, dataStorage, filesInSystem);
                storeRequestThread.start();

            } else if (snReceive.getType() == Clientproto.SNReceive.packetType.RETRIEVE) {
                RetrieveRequestThread retrieveRequestThread = new RetrieveRequestThread(socket, snReceive, balancedHashRing, nodeId, dataStorage, filesInSystem);
                retrieveRequestThread.start();

            } else if (snReceive.getType() == Clientproto.SNReceive.packetType.SYSTEM) {
                systemResponse();
                System.out.println("Received system message");

            } else if (snReceive.getType() == Clientproto.SNReceive.packetType.BROADCAST) {
                if (snReceive.getSendBroadCast()) {
                    sendBroadcast(snReceive.getFileData().getFilename());
                }
                System.out.println("Adding file to known files list: " + snReceive.getFileData().getFilename());
                filesInSystem.add(snReceive.getFileData().getFilename());

            } else if (snReceive.getType() == Clientproto.SNReceive.packetType.PIPELINE) {
                PipelineRequestThread pipelineRequestThread = new PipelineRequestThread(socket, snReceive, balancedHashRing, nodeId, dataStorage);
                pipelineRequestThread.start();

            }
        }
    }

    private void systemResponse(){
        Clientproto.SNReceive reply =  Clientproto.SNReceive.newBuilder().addAllNodeFiles(filesInSystem).build();
        try {
            OutputStream outstream = socket.getOutputStream();
            reply.writeDelimitedTo(outstream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendBroadcast(String filename){
        //Todo have thread manager
        Clientproto.SNReceive broadcastMessage = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.BROADCAST).setFileData(Clientproto.FileData.newBuilder().setFilename(filename).build()).setSendBroadCast(false).build();
        CountDownLatch latch = new CountDownLatch(storageNodeInfos.size());
        System.out.println("Number of nodes to send to: " + storageNodeInfos.size());

        for(Object entry : balancedHashRing.getEntryList()){
            HashRingEntry currEntry = (HashRingEntry) entry;
            SendBroadCastThread sendBroadCastThread = new SendBroadCastThread(latch,currEntry,broadcastMessage);
            sendBroadCastThread.start();
        }

        try {
            latch.await();
            System.out.println("Broadcast success");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
