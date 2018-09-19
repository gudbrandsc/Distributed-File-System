import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HearbeatThread extends Thread {
    private String coordIp;
    private int coordPort;
    private String myIp;
    private int myPort;
    private HashMap<String, StorageNodeInfo> storageNodes;
    private volatile boolean alive;



    /** Constructor */
    public HearbeatThread(String coordIp, int coordPort, String myIp, int myPort, HashMap<String,StorageNodeInfo> storageNodes) {
        this.coordIp = coordIp;
        this.coordPort = coordPort;
        this.myIp = myIp;
        this.myPort = myPort;
        this.storageNodes = storageNodes;
        this.storageNodes = storageNodes;
        this.alive = true;

    }

    /**
     * Method that sends GET requests to check if a node is alive.
     * If user service is a master then it will check all secondaries and frontend's.
     * If user service is a secondary it will check if the master is alive.
     * Sends heartbeats every 5 seconds
     */
    public void run() {
        Socket socket = null;
        Clientproto.CordResponse reply = null;

        while (alive) {
            try {
                socket = new Socket(coordIp, coordPort);
                InputStream instream = socket.getInputStream();
                OutputStream outstream = socket.getOutputStream();
                Clientproto.CordReceive heartBeatMessage = Clientproto.CordReceive.newBuilder().setType(Clientproto.CordReceive.packetType.HEARTBEAT).setIp(myIp).setPort(myPort).build();
                heartBeatMessage.writeDelimitedTo(outstream);
                reply = Clientproto.CordResponse.parseDelimitedFrom(instream);

                for(Clientproto.NodeInfo nodeInfo : reply.getRemovedNodesList()){
                    System.out.println("Removed node: " +nodeInfo.getIp() + ":" + nodeInfo.getPort());
                    storageNodes.remove(nodeInfo.getIp()+nodeInfo.getPort());
                }

                for(Clientproto.NodeInfo nodeInfo : reply.getNewNodesList()){
                    System.out.println("Added node: " +nodeInfo.getIp() + ":" + nodeInfo.getPort());
                    storageNodes.put(nodeInfo.getIp()+nodeInfo.getPort(), new StorageNodeInfo(nodeInfo.getIp(),nodeInfo.getPort(),10,10,nodeInfo.getId()));
                }

            } catch (IOException e) {
                e.printStackTrace();
            }                //TODO check reply
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }
}
