import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Gudbrand Schistad
 * Thread class that is used to print a message recived from an other user, and send a reply.
 */
public class MessageListener extends Thread{
    private Socket socket;
    private HashMap<String, StorageNodeInfo> storageNodeInfos;
    private AtomicInteger nodeId;
    private int totAvailableSpace;
    private int totRequestHandled;

    /**Constructor*/
    MessageListener(Socket socket, HashMap<String,StorageNodeInfo> storageNodeInfos, AtomicInteger nodeId, int totRequestHandled, int totAvailableSpace) {
        this.socket = socket;
        this.totAvailableSpace = totAvailableSpace;
        this.totRequestHandled = totRequestHandled;
        this.storageNodeInfos = storageNodeInfos;
        this.nodeId = nodeId;
    }

    /**
     * Run method that prints the message and sends a reply
     */
    public void run() {
        Clientproto.CordReceive cordReceive  = null;

        try {
            cordReceive = Clientproto.CordReceive.parseDelimitedFrom(socket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(cordReceive != null){
            if(cordReceive.getType() == Clientproto.CordReceive.packetType.SYSTEM) {
                System.out.println("System message");

                SystemReportResponder systemReportResponder = new SystemReportResponder(socket, storageNodeInfos, totAvailableSpace, totRequestHandled);
                systemReportResponder.start();
            } else if (cordReceive.getType() == Clientproto.CordReceive.packetType.JOIN){
                System.out.println("Join request");
                //todo Should be sync
                StorageNodeJoinRequest storageNodeJoinRequest = new StorageNodeJoinRequest(socket, storageNodeInfos, cordReceive, nodeId);
                storageNodeJoinRequest.start();

            }else if (cordReceive.getType() == Clientproto.CordReceive.packetType.HEARTBEAT){
                StorageNodeInfo storageNodeInfo = storageNodeInfos.get(cordReceive.getIp() + cordReceive.getPort());
                storageNodeInfo.setHeartbeatReceived();
                HeartbeatResponder heartbeatResponder = new HeartbeatResponder(socket, storageNodeInfo);
                heartbeatResponder.start();
            }
        }
    }
}
