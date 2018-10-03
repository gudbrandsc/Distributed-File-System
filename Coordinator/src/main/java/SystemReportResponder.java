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


    /**Constructor*/
    SystemReportResponder(Socket socket, BalancedHashRing balancedHashRing) {
        this.socket = socket;
        this.balancedHashRing = balancedHashRing;
    }

    /**
     * Run method that prints the message and sends a reply
     */
    public void run() {

        ArrayList<Clientproto.NodeInfo> nodeInfos = new ArrayList<Clientproto.NodeInfo>();

        ArrayList<HashRingEntry> hashRingEntries = new ArrayList<>(balancedHashRing.getEntryMap().values());
        for(HashRingEntry entry : hashRingEntries){
            Clientproto.BInteger.Builder builder = Clientproto.BInteger.newBuilder();
            ByteString bytes = ByteString.copyFrom(entry.getPosition().toByteArray());
            builder.setPosition(bytes);
            nodeInfos.add(Clientproto.NodeInfo.newBuilder().setPosition(builder.build()).setIp(entry.getIp()).setPort(entry.getPort()).setId(entry.getNodeId()).setNeighbor(entry.getNeighbor().getNodeId()).build());

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


