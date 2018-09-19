import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Gudbrand Schistad
 * Thread class that is used to print a message recived from an other user, and send a reply.
 */
public class MessageListener extends Thread{
    private Socket socket;
    private HashMap<String, StorageNodeInfo> storageNodeInfos;
    private int nodeId;
    private int totAvailableSpace;
    private int totRequestHandled;

    /**Constructor*/
    MessageListener(Socket socket, HashMap<String,StorageNodeInfo> storageNodeInfos, int nodeId, int totRequestHandled, int totAvailableSpace) {
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
        Clientproto.SNReceive snReceive  = null;

        try {
            snReceive = Clientproto.SNReceive.parseDelimitedFrom(socket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(snReceive != null){
            if(snReceive.getType() == Clientproto.SNReceive.packetType.STORE) {
                System.out.println("Received store message");

            } else if (snReceive.getType() == Clientproto.SNReceive.packetType.RETRIEVE){
                System.out.println("Received retrieve request");

            }else if (snReceive.getType() == Clientproto.SNReceive.packetType.SYSTEM){
                System.out.println("Received system message");
            }
        }
    }
}
