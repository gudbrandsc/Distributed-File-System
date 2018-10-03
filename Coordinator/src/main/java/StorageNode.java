import Hash.BalancedHashRing;
import Hash.HashRingEntry;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

public class StorageNode {

    private HeartbeatThread heartbeatThread;
    private String ip;
    private int port;
    private int nodeId;
    private ArrayList<HashRingEntry> newEntries;
    private ArrayList<HashRingEntry> removedNodes;

    public StorageNode(HashMap<String, StorageNode> storageNodeMap, BalancedHashRing balancedHashRing, int nodeId, String ip, int port, BigInteger position){
        this.ip = ip;
        this.port = port;
        this.nodeId = nodeId;
        heartbeatThread = new HeartbeatThread(storageNodeMap, balancedHashRing, ip, port, position);
        heartbeatThread.start();
        this.newEntries = new ArrayList<>();
        this.removedNodes = new ArrayList<>();
    }

    public void addRemovedRingEntry(HashRingEntry entry){
        this.removedNodes.add(entry);
    }

    public void addNewRingEntry(HashRingEntry entry){
        this.newEntries.add(entry);
    }

    public void clearRingList(){
        this.newEntries.clear();
        this.removedNodes.clear();
    }

    public boolean newRingEntry(){
        if(this.newEntries.isEmpty()){
            return false;
        }
        return true;
    }

    public boolean newRemovedRingEntry(){
        if(this.removedNodes.isEmpty()){
            return false;
        }
        return true;
    }



    public ArrayList<HashRingEntry> getNewEntries() {
        return newEntries;
    }
    public ArrayList<HashRingEntry> getRemovedNodes() {
        return removedNodes;
    }

    public void setHeartbeatReceived(){
        this.heartbeatThread.setNewHeartBeat();
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public int getNodeId() {
        return nodeId;
    }
}
