import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * Class used to update information about the system.*/
public class UpdateSystemInfoThread extends Thread{
    private static ArrayList<StorageNodeInfo> storageNodes;
    private CountDownLatch latch;
    private String coordIp;
    private int coordPort;

    /** Constructor */
    public UpdateSystemInfoThread(ArrayList<StorageNodeInfo> storageNodes, CountDownLatch latch, int coordPort, String coordIp){
        this.storageNodes = storageNodes;
        this.latch = latch;
        this.coordIp = coordIp;
        this.coordPort = coordPort;
    }

    public void run(){
        Socket socket = null;
        Clientproto.CordResponse reply = null;

        try {
            socket = new Socket(coordIp, coordPort);
            InputStream instream = socket.getInputStream();
            OutputStream outstream = socket.getOutputStream();

            Clientproto.CordReceive message = Clientproto.CordReceive.newBuilder().setType(Clientproto.CordReceive.packetType.SYSTEM).build();
            message.writeDelimitedTo(outstream);
            reply = Clientproto.CordResponse.parseDelimitedFrom(instream);

            storageNodes.clear();
            for(Clientproto.NodeInfo nodeInfo : reply.getNewNodesList()){
                StorageNodeInfo node = new StorageNodeInfo(nodeInfo.getIp(), nodeInfo.getPort(), nodeInfo.getId(), nodeInfo.getAvailSpace(),nodeInfo.getTotRequest());
                storageNodes.add(node);
            }

        } catch (IOException e) {
            System.out.println("Unable to contact coordinator");
            e.printStackTrace();

        }
        latch.countDown();
    }
}
