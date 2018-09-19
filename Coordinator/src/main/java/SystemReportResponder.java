import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Gudbrand Schistad
 * Thread class that is used to print a message recived from an other user, and send a reply.
 */
public class SystemReportResponder extends Thread{
    private Socket socket;
    private HashMap<String,StorageNodeInfo> storageNodeInfos;
    private int available_space;
    private int request_handled;

    /**Constructor*/
    SystemReportResponder(Socket socket, HashMap<String,StorageNodeInfo> storageNodeInfos, int available_space, int request_handled) {
        this.socket = socket;
        this.available_space = available_space;
        this.request_handled = request_handled;
        this.storageNodeInfos = storageNodeInfos;
    }

    /**
     * Run method that prints the message and sends a reply
     */
    public void run() {
        ArrayList<Clientproto.NodeInfo> nodeInfos = new ArrayList<Clientproto.NodeInfo>();

        for(StorageNodeInfo nodeInfo : storageNodeInfos.values()){
            nodeInfos.add(Clientproto.NodeInfo.newBuilder().setIp(nodeInfo.getIp()).setPort(nodeInfo.getPort()).setId(nodeInfo.getId()).build());

        }

        Clientproto.CordResponse reply = Clientproto.CordResponse.newBuilder().setAvailSpace(this.available_space).setReqHandled(request_handled).addAllAllNodes(nodeInfos).build();
        try {
            reply.writeDelimitedTo(socket.getOutputStream());
            socket.getOutputStream().flush();
            socket.getOutputStream().close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}


