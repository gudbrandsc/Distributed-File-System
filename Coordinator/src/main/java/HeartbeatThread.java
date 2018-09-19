
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class HeartbeatThread extends Thread{
    private volatile boolean newHeartBeat;
    private String myip;
    private int myPort;
    private HashMap<String, StorageNodeInfo> storageNodes;
    private StorageNodeInfo storageNodeInfo;


    public HeartbeatThread(String myip, int myPort, HashMap<String, StorageNodeInfo> storageNodes, StorageNodeInfo storageNodeInfo) {
        this.newHeartBeat = true;
        this.myip = myip;
        this.myPort = myPort;
        this.storageNodes = storageNodes;
        this.storageNodeInfo = storageNodeInfo;
    }

    /**
     * Method that sends GET requests to check if a node is alive.
     * If user service is a master then it will check all secondaries and frontend's.
     * If user service is a secondary it will check if the master is alive.
     * Sends heartbeats every 5 seconds
     */
    public void run() {
        System.out.println("Started waiting for heartbeats");
        while (newHeartBeat) {
            this.newHeartBeat = false;
            try {
                TimeUnit.SECONDS.sleep(7);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Did not received heartbeat for node: " + myip + myPort);
        storageNodes.remove(myip + ":" + myPort);
        removeNewNodeToHeartbeats(storageNodeInfo);
    }

    public void setNewHeartBeat(){
        this.newHeartBeat = true;
    }

    public void removeNewNodeToHeartbeats(StorageNodeInfo storageNodeInfo){
        for(StorageNodeInfo node: storageNodes.values()){
            node.addRemovedNode(storageNodeInfo);
        }
    }
}
