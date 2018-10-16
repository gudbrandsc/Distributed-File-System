import Hash.HashRingEntry;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;

public class HeartbeatResponder extends Thread{
    private Socket socket;
    private StorageNode storageNode;
    private Clientproto.CordReceive cordReceive;


    /**Constructor*/
    HeartbeatResponder(Socket socket, StorageNode storageNode, Clientproto.CordReceive cordReceive ) {
        this.socket = socket;
        this.storageNode = storageNode;
        this.cordReceive = cordReceive;
    }

    /**
     * Run method that prints the message and sends a reply
     */
    public void run() {
        storageNode.setAvailableSpace(cordReceive.getAvailSpace());
        storageNode.setRequestHandled(cordReceive.getReqHandled());

        Clientproto.CordResponse reply = null;
        ArrayList<Clientproto.NodeInfo> newRingEntries = new ArrayList<Clientproto.NodeInfo>();
        ArrayList<Clientproto.NodeInfo> newRemovedRingEntries = new ArrayList<Clientproto.NodeInfo>();

        if(storageNode.newRingEntry()){
            for(HashRingEntry entry : storageNode.getNewEntries()){
                Clientproto.BInteger.Builder builder = Clientproto.BInteger.newBuilder();
                ByteString bytes = ByteString.copyFrom(entry.getPosition().toByteArray());
                builder.setPosition(bytes);
                newRingEntries.add(Clientproto.NodeInfo.newBuilder().setPosition(builder.build()).setIp(entry.getIp()).setPort(entry.getPort()).setId(entry.getNodeId()).setNeighbor(entry.neighbor.getNodeId()).build());
            }
        }

        if(storageNode.newRemovedRingEntry()){
            for(HashRingEntry entry : storageNode.getRemovedNodes()){
                Clientproto.BInteger.Builder builder = Clientproto.BInteger.newBuilder();
                ByteString bytes = ByteString.copyFrom(entry.getPosition().toByteArray());
                builder.setPosition(bytes);
                newRemovedRingEntries.add(Clientproto.NodeInfo.newBuilder().setPosition(builder.build()).build());
                System.out.println("Added removed node at position: " + entry.getPosition());
            }
        }
        reply = Clientproto.CordResponse.newBuilder().setType(Clientproto.CordResponse.packetType.HEARTBEAT).addAllNewNodes(newRingEntries).addAllRemovedNodes(newRemovedRingEntries).build();


        try {
            reply.writeDelimitedTo(socket.getOutputStream());
            storageNode.clearRingList();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
