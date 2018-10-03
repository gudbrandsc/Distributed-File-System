import Hash.BalancedHashRing;
import Hash.HashException;
import Hash.HashRingEntry;
import Hash.HashTopologyException;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.math.BigInteger;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class StorageNodeJoinRequest extends Thread{
    private Socket socket;
    private HashMap<String, StorageNode> storageNodeMap;
    private Clientproto.CordReceive cordReceive;
    private AtomicInteger nodeId;
    private BalancedHashRing balancedHashRing;
    /**Constructor*/
    StorageNodeJoinRequest(Socket socket, HashMap<String, StorageNode> storageNodeMap, Clientproto.CordReceive cordReceive, AtomicInteger nodeId, BalancedHashRing balancedHashRing) {
        this.socket = socket;
        this.storageNodeMap = storageNodeMap;
        this.cordReceive = cordReceive;
        this.nodeId = nodeId;
        this.balancedHashRing = balancedHashRing;

    }

    /**
     * Run method that prints the message and sends a reply
     */
    public void run() {
        //TODO Start thread to listen for heartbeat from that node and add the node to queue
        Clientproto.CordResponse reply = null;
        if(storageNodeMap.containsKey(cordReceive.getIp() + ":" + cordReceive.getPort())){
            reply = Clientproto.CordResponse.newBuilder().setCanJoin(false).build();
        }else{
           int newId = nodeId.incrementAndGet();
            //TODO Add to hash ring
            BigInteger newNodePos = null;
            try {
                newNodePos = balancedHashRing.addNode(newId, cordReceive.getIp(), cordReceive.getPort());
            } catch (HashTopologyException e) {
                e.printStackTrace();
            } catch (HashException e) {
                e.printStackTrace();
            }

            ArrayList<Clientproto.NodeInfo> nodeInfos = new ArrayList<Clientproto.NodeInfo>();
            ArrayList<HashRingEntry> hashRingEntries = new ArrayList<>(balancedHashRing.getEntryMap().values());


            for(HashRingEntry entry : hashRingEntries){
                Clientproto.BInteger.Builder builder = Clientproto.BInteger.newBuilder();
                ByteString bytes = ByteString.copyFrom(entry.getPosition().toByteArray());
                builder.setPosition(bytes);
                nodeInfos.add(Clientproto.NodeInfo.newBuilder().setPosition(builder.build()).setIp(entry.getIp()).setPort(entry.getPort()).setId(entry.getNodeId()).setNeighbor(entry.neighbor.getNodeId()).build());

            }

            reply = Clientproto.CordResponse.newBuilder().addAllNewNodes(nodeInfos).setCanJoin(true).setNodeId(newId).setType(Clientproto.CordResponse.packetType.JOIN).build();
            StorageNode storageNode = new StorageNode(storageNodeMap, balancedHashRing, newId, cordReceive.getIp(), cordReceive.getPort(), newNodePos);
            addNewNodeToHeartbeats(cordReceive.getIp(), cordReceive.getPort(), storageNode, newNodePos);

        }
        try {
            reply.writeDelimitedTo(socket.getOutputStream());
            socket.getOutputStream().flush();
            socket.getOutputStream().close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //TODO Create class to store this map and sync
    public void addNewNodeToHeartbeats(String ip, int port, StorageNode storageNode, BigInteger newPos){
        HashRingEntry newEntry = balancedHashRing.getEntryAtPos(newPos);
        for(StorageNode node : storageNodeMap.values()){
            node.addNewRingEntry(newEntry);
        }
        storageNodeMap.put(ip+port, storageNode);
    }
}
