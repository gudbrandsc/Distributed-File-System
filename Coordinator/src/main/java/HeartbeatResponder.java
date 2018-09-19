import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class HeartbeatResponder extends Thread{
    private Socket socket;
    private StorageNodeInfo currNode;


    /**Constructor*/
    HeartbeatResponder(Socket socket, StorageNodeInfo storageNodeInfo) {
        this.socket = socket;
        this.currNode = storageNodeInfo;
    }

    /**
     * Run method that prints the message and sends a reply
     */
    public void run() {
        ArrayList<Clientproto.NodeInfo> newNodes = new ArrayList<Clientproto.NodeInfo>();
        ArrayList<Clientproto.NodeInfo> removeNodes = new ArrayList<Clientproto.NodeInfo>();

        for(StorageNodeInfo nodeInfo : currNode.getNewNodes()){
            System.out.println("loop");
            newNodes.add(Clientproto.NodeInfo.newBuilder().setIp(nodeInfo.getIp()).setPort(nodeInfo.getPort()).setId(nodeInfo.getId()).build());

        }
        for(StorageNodeInfo nodeInfo : currNode.getRemovedNodes()){
            removeNodes.add(Clientproto.NodeInfo.newBuilder().setIp(nodeInfo.getIp()).setPort(nodeInfo.getPort()).setId(nodeInfo.getId()).build());

        }

        Clientproto.CordResponse reply = Clientproto.CordResponse.newBuilder().setType(Clientproto.CordResponse.packetType.HEARTBEAT).addAllRemovedNodes(removeNodes).addAllNewNodes(newNodes).build();
        try {
            reply.writeDelimitedTo(socket.getOutputStream());
            currNode.clearLists();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
