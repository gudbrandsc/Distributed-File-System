import Hash.BalancedHashRing;
import Hash.HashRingEntry;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.math.BigInteger;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Gudbrand Schistad
 * Thread class that is used to print a message recived from an other user, and send a reply.
 */
public class SystemReportResponder extends Thread{
    private Socket socket;
    private BalancedHashRing balancedHashRing;
    private HashMap<String, StorageNode> storageNodeMap;



    /**Constructor*/
    SystemReportResponder(Socket socket, BalancedHashRing balancedHashRing, HashMap<String, StorageNode> storageNodeMap) {
        this.socket = socket;
        this.balancedHashRing = balancedHashRing;
        this.storageNodeMap = storageNodeMap;
    }

    /**
     * Run method that prints the message and sends a reply
     */
    public void run() {

        ArrayList<Clientproto.NodeInfo> nodeInfos = new ArrayList<Clientproto.NodeInfo>();

        ArrayList<HashRingEntry> hashRingEntries = new ArrayList<>(balancedHashRing.getEntryMap().values());

        for(HashRingEntry entry : hashRingEntries){
            StorageNode node = storageNodeMap.get(entry.getIp() + entry.getPort());
            System.out.println("Adding node with available space: " + node.getAvailableSpace() + " and id " + node.getNodeId());
            nodeInfos.add(Clientproto.NodeInfo.newBuilder().setIp(entry.getIp()).setPort(entry.getPort()).setId(entry.getNodeId()).setAvailSpace(node.getAvailableSpace()).setTotRequest(node.getRequestHandled()).build());

        }

        Clientproto.CordResponse reply = Clientproto.CordResponse.newBuilder().addAllNewNodes(nodeInfos).build();
        try {
            reply.writeDelimitedTo(socket.getOutputStream());
            socket.getOutputStream().flush();
            socket.getOutputStream().close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}


