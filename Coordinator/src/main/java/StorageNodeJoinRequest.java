import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class StorageNodeJoinRequest extends Thread{
    private Socket socket;
    private HashMap<String, StorageNodeInfo> storageNodeInfos;
    private Clientproto.CordReceive cordReceive;
    private AtomicInteger nodeId;

/**Constructor*/
    StorageNodeJoinRequest(Socket socket, HashMap<String, StorageNodeInfo> storageNodeInfos, Clientproto.CordReceive cordReceive, AtomicInteger nodeId) {
        this.socket = socket;
        this.storageNodeInfos = storageNodeInfos;
        this.cordReceive = cordReceive;
        this.nodeId = nodeId;

    }

    /**
     * Run method that prints the message and sends a reply
     */
    public void run() {
        //TODO Start thread to listen for heartbeat from that node and add the node to queue
        Clientproto.CordResponse reply = null;
        if(storageNodeInfos.containsKey(cordReceive.getIp() + ":" + cordReceive.getPort())){
            reply = Clientproto.CordResponse.newBuilder().setCanJoin(false).build();

        }else{
            //TODO add more data
            reply = Clientproto.CordResponse.newBuilder().setCanJoin(true).build();
            System.out.println("Adding : " + cordReceive.getIp() + ":" + cordReceive.getPort());
            StorageNodeInfo storageNode = new StorageNodeInfo(cordReceive.getIp(),cordReceive.getPort(),10,10, nodeId.getAndIncrement(), storageNodeInfos);
            addNewNodeToHeartbeats(storageNode);
            for(StorageNodeInfo node : storageNodeInfos.values()){
                storageNode.addNewNode(node);
            }
            storageNodeInfos.put(cordReceive.getIp() + cordReceive.getPort(), storageNode);


        }
        try {
            reply.writeDelimitedTo(socket.getOutputStream());
            socket.getOutputStream().flush();
            socket.getOutputStream().close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addNewNodeToHeartbeats(StorageNodeInfo storageNodeInfo){
        for(StorageNodeInfo node: storageNodeInfos.values()){
            node.addNewNode(storageNodeInfo);
        }
    }
}
