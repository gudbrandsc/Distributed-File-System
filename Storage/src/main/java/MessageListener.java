import Hash.BalancedHashRing;
import Hash.HashRingEntry;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Gudbrand Schistad
 * Thread class that is used to print a message recived from an other user, and send a reply.
 */
public class MessageListener extends Thread {
    private Socket socket;
    private static HashMap<String, StorageNodeInfo> storageNodeInfos;
    private int nodeId;
    private BalancedHashRing balancedHashRing;
    private SystemDataStore systemDataStore;


    /**
     * Constructor
     */
    MessageListener(Socket socket, HashMap<String, StorageNodeInfo> storageNodeInfos, int nodeId, BalancedHashRing balancedHashRing, SystemDataStore systemDataStore) {
        this.socket = socket;
        this.storageNodeInfos = storageNodeInfos;
        this.nodeId = nodeId;
        this.balancedHashRing = balancedHashRing;
        this.systemDataStore = systemDataStore;

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
            systemDataStore.getTotRequestsHandled().incrementAndGet();
            if (snReceive.getType() == Clientproto.SNReceive.packetType.STORE) {
                StoreRequestThread storeRequestThread = new StoreRequestThread(socket, snReceive, balancedHashRing, nodeId, systemDataStore);
                storeRequestThread.start();
            } else if (snReceive.getType() == Clientproto.SNReceive.packetType.RETRIEVE) {
                RetrieveRequestThread retrieveRequestThread = new RetrieveRequestThread(socket, snReceive, balancedHashRing, nodeId, systemDataStore);
                retrieveRequestThread.start();
            } else if (snReceive.getType() == Clientproto.SNReceive.packetType.SYSTEM) {
                systemResponse();
            } else if (snReceive.getType() == Clientproto.SNReceive.packetType.BROADCAST) {
                if (snReceive.getSendBroadCast()) {
                    sendBroadcast(snReceive.getFileData().getFilename());
                }
                System.out.println("Adding file to known files list: " + snReceive.getFileData().getFilename());
                systemDataStore.addFilesInSystem(snReceive.getFileData().getFilename());

            } else if (snReceive.getType() == Clientproto.SNReceive.packetType.PIPELINE) {
                PipelineRequestThread pipelineRequestThread = new PipelineRequestThread(socket, snReceive, balancedHashRing, nodeId, systemDataStore);
                pipelineRequestThread.start();
            } else if (snReceive.getType() == Clientproto.SNReceive.packetType.CHECKSUM) {
                ChecksumResponder checksumResponder = new ChecksumResponder(socket,snReceive,nodeId);
                checksumResponder.start();
            }else if (snReceive.getType() == Clientproto.SNReceive.packetType.RECOVER) {
                ArrayList<Clientproto.NodeInfo> nodeInfos = new ArrayList<>();

                for(Object item : balancedHashRing.getEntryList()){
                    System.out.println("Adding node ro recover");
                    HashRingEntry entry = (HashRingEntry) item;
                    Clientproto.BInteger.Builder builder = Clientproto.BInteger.newBuilder();
                    ByteString bytes = ByteString.copyFrom(entry.getPosition().toByteArray());
                    builder.setPosition(bytes);
                    nodeInfos.add(Clientproto.NodeInfo.newBuilder().setPosition(builder.build()).setIp(entry.getIp()).setPort(entry.getPort()).setId(entry.getNodeId()).setNeighbor(entry.neighbor.getNodeId()).build());
                }

                Clientproto.CordResponse reply = Clientproto.CordResponse.newBuilder().addAllNewNodes(nodeInfos).build();

                try {
                    reply.writeDelimitedTo(socket.getOutputStream());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    private void systemResponse(){
        ArrayList<Clientproto.FileData> fileData = new ArrayList<>();

        for(Clientproto.SNReceive value : systemDataStore.getDataStoreCopy().values()){
            Clientproto.FileData data = value.getFileData();
            Clientproto.FileData newData = Clientproto.FileData.newBuilder().setChunkNo(data.getChunkNo()).setReplicaNum(data.getReplicaNum()).setFilename(data.getFilename()).setNumChunks(data.getNumChunks()).build();
            fileData.add(newData);
        }

        Clientproto.SNReceive reply =  Clientproto.SNReceive.newBuilder().addAllNodeFiles(systemDataStore.getFilesInSystem()).addAllChunkList(fileData).build();
        try {
            OutputStream outstream = socket.getOutputStream();
            reply.writeDelimitedTo(outstream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendBroadcast(String filename){
        Clientproto.SNReceive broadcastMessage = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.BROADCAST).setFileData(Clientproto.FileData.newBuilder().setFilename(filename).build()).setSendBroadCast(false).build();
        CountDownLatch latch = new CountDownLatch(balancedHashRing.getEntryList().size());
        System.out.println("Number of nodes to send to: " + balancedHashRing.getEntryList().size());

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
