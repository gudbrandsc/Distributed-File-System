import java.util.ArrayList;
import java.util.HashMap;

public class StorageNodeInfo {
    private String ip;
    private int port;
    private int available_space;
    private int num_request;
    private int id;
    private HeartbeatThread heartbeatThread;
    private HashMap<String, StorageNodeInfo> storageNodes;
    private ArrayList<StorageNodeInfo> newNodes;
    private ArrayList<StorageNodeInfo> removedNodes;


    public StorageNodeInfo(String ip, int port, int available_space, int num_request, int id, HashMap<String, StorageNodeInfo> storageNodes){
        this.ip = ip;
        this.port = port;
        this.available_space = available_space;
        this.num_request = num_request;
        this.id = id;
        this.storageNodes = storageNodes;
        newNodes = new ArrayList<StorageNodeInfo>();
        removedNodes = new ArrayList<StorageNodeInfo>();
        heartbeatThread = new HeartbeatThread(ip, port, storageNodes, this);
        heartbeatThread.start();
    }

    public String getIp() {
        return this.ip;
    }

    public int getPort() {
        return this.port;
    }

    public int getAvailable_space() {
        return available_space;
    }

    public int getNum_request() {
        return num_request;
    }

    public synchronized void setAvailable_space(int available_space) {
        this.available_space = available_space;
    }

    public synchronized void setNum_request(int num_request) {
        this.num_request = num_request;
    }

    public int getId() {
        return this.id;
    }

    public void setHeartbeatReceived(){
        this.heartbeatThread.setNewHeartBeat();
    }
    public void addRemovedNode(StorageNodeInfo storageNodeInfo){
        removedNodes.add(storageNodeInfo);
    }

    public void addNewNode(StorageNodeInfo storageNodeInfo){
        newNodes.add(storageNodeInfo);
    }

    public ArrayList<StorageNodeInfo> getNewNodes() {
        return newNodes;
    }

    public ArrayList<StorageNodeInfo> getRemovedNodes() {
        return removedNodes;
    }

    public void clearLists(){
        this.newNodes.clear();
        this.removedNodes.clear();
    }
}
