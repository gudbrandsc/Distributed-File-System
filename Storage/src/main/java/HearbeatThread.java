import Hash.BalancedHashRing;
import Hash.HashException;
import Hash.HashRingEntry;
import Hash.HashTopologyException;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HearbeatThread extends Thread {
    private String coordIp;
    private int coordPort;
    private String myIp;
    private int myPort;
    private static int nodeid;
    private static HashMap<String, StorageNodeInfo> storageNodes;
    private volatile boolean alive;
    private static BalancedHashRing balancedHashRing;
    private SystemDataStore systemDataStore;

    /** Constructor */
    public HearbeatThread(String coordIp, int coordPort, String myIp, int myPort, HashMap<String, StorageNodeInfo> storageNodes, BalancedHashRing balancedHashRing,int nodeid, SystemDataStore systemDataStore) {
        this.coordIp = coordIp;
        this.coordPort = coordPort;
        this.myIp = myIp;
        this.myPort = myPort;
        this.storageNodes = storageNodes;
        this.alive = true;
        this.balancedHashRing = balancedHashRing;
        this.nodeid = nodeid;
        this.systemDataStore = systemDataStore;



    }

    /**
     * Method that sends GET requests to check if a node is alive.
     * If user service is a master then it will check all secondaries and frontend's.
     * If user service is a secondary it will check if the master is alive.
     * Sends heartbeats every 5 seconds
     */
    public void run() {
        Socket socket = null;
        Clientproto.CordResponse reply = null;
        while (alive) {
            try {
                socket = new Socket(coordIp, coordPort);
                InputStream instream = socket.getInputStream();
                OutputStream outstream = socket.getOutputStream();
                Clientproto.CordReceive heartBeatMessage = Clientproto.CordReceive.newBuilder().setType(Clientproto.CordReceive.packetType.HEARTBEAT).setIp(myIp).setPort(myPort).setAvailSpace(systemDataStore.getTotAvailableSpace()).setReqHandled(systemDataStore.getTotRequestsHandled().intValue()).build();
                heartBeatMessage.writeDelimitedTo(outstream);
                reply = Clientproto.CordResponse.parseDelimitedFrom(instream);
                if(reply.getNewNodesList().size() != 0){
                    try {
                        addAllNodes(reply);
                    } catch (HashTopologyException e) {
                        e.printStackTrace();
                    }

                }
                if(reply.getRemovedNodesList().size() != 0){
                        removeAllNodes(reply);
                }



            } catch (IOException e) {
                System.out.println("Unable to contact coordinator");
                //e.printStackTrace();
            }                //TODO check reply
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void removeAllNodes(Clientproto.CordResponse reply){
        ArrayList<Clientproto.NodeInfo> removeNodeList = new ArrayList<>(reply.getRemovedNodesList());
        for (Clientproto.NodeInfo node : removeNodeList){
            ByteString bytes = node.getPosition().getPosition();
            BigInteger position = new BigInteger(bytes.toByteArray());
            HashRingEntry entry = balancedHashRing.getEntry(position);

            if(entry.neighbor.getNodeId() == nodeid){
                OldNeighborRehashThread oldNeighborRehashThread = new OldNeighborRehashThread(systemDataStore, balancedHashRing, balancedHashRing.getEntryById(nodeid).position);
                balancedHashRing.removeRingEntry(position);
                oldNeighborRehashThread.start();

            }else if(balancedHashRing.getEntryById(nodeid).neighbor == entry){
                PredecessorRehashThread predecessorRehashThread = new PredecessorRehashThread(systemDataStore,balancedHashRing,balancedHashRing.getEntryById(nodeid).position);
                balancedHashRing.removeRingEntry(position);
                predecessorRehashThread.start();
            }else{
                balancedHashRing.removeRingEntry(position);
            }
        }
    }

    private void addAllNodes(Clientproto.CordResponse reply) throws HashTopologyException {
        TreeMap<BigInteger, Clientproto.NodeInfo> tempTree = new TreeMap<>();
        for( Clientproto.NodeInfo node: reply.getNewNodesList()) {
            ByteString bytes = node.getPosition().getPosition();
            BigInteger position = new BigInteger(bytes.toByteArray());
            tempTree.put(position, node);
            StorageNodeInfo storageNode = new StorageNodeInfo(node.getIp(),node.getPort(),0,0,node.getId());
            storageNodes.put(node.getIp()+node.getPort(), storageNode);
        }

        for(Map.Entry<BigInteger,Clientproto.NodeInfo> entry : tempTree.entrySet()) {
            BigInteger key = entry.getKey();
            Clientproto.NodeInfo value = entry.getValue();
            try {
                System.out.println("Trying to add entry with posistion: " + key);
                balancedHashRing.addNodeWithPosition(key, value.getId(), value.getIp(), value.getPort());

                if(balancedHashRing.getEntryById(value.getId()).neighbor.getNodeId() == nodeid){
                    NewRingEntryRehasingThread newRingEntryRehasingThread = new NewRingEntryRehasingThread(balancedHashRing.getEntryById(value.getId()), balancedHashRing, systemDataStore);
                    newRingEntryRehasingThread.start();
                }

            } catch (HashException e) {
                e.printStackTrace();
            }
        }
    }
}
