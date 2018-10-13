
import Hash.BalancedHashRing;

import java.math.BigInteger;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class HeartbeatThread extends Thread{
    private volatile boolean newHeartBeat;
    private BalancedHashRing balancedHashRing;
    private HashMap<String, StorageNode> storageNodeMap;
    private int port;
    private String ip;
    private BigInteger position;

    public HeartbeatThread(HashMap<String, StorageNode> storageNodeMap, BalancedHashRing balancedHashRing, String ip, int port, BigInteger position) {
        this.balancedHashRing = balancedHashRing;
        this.storageNodeMap = storageNodeMap;
        this.newHeartBeat = true;
        this.ip = ip;
        this.port = port;
        this.position = position;
    }

    /**
     * Method that sends GET requests to check if a node is alive.
     * If user service is a master then it will check all secondaries and frontend's.
     * If user service is a secondary it will check if the master is alive.
     * Sends heartbeats every 5 seconds
     */
    public void run() {
//        System.out.println("Started waiting for heartbeats");
        while (newHeartBeat) {
            this.newHeartBeat = false;
            try {
                TimeUnit.SECONDS.sleep(7);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        storageNodeMap.remove(ip + port);
        //Inform all nodes that this node is removed
        for(StorageNode node : storageNodeMap.values()){
            node.addRemovedRingEntry(balancedHashRing.getEntryAtPos(position));
        }

        balancedHashRing.removeRingEntry(position);


        System.out.println("Did not receive heartbeat remove this object");
    }

    public void setNewHeartBeat(){
        this.newHeartBeat = true;
    }

}
