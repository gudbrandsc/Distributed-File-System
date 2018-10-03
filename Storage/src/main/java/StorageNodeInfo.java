import Hash.HashRingEntry;

import java.util.ArrayList;

public class StorageNodeInfo {
    private String ip;
    private int port;
    private int available_space;
    private int num_request;
    private int id;
    private ArrayList<HashRingEntry> newNodes;
    private ArrayList<HashRingEntry> removedNodes;

    public StorageNodeInfo(String ip, int port, int available_space, int num_request, int id){
        this.ip = ip;
        this.port = port;
        this.available_space = available_space;
        this.num_request = num_request;
        this.id = id;
        this.newNodes = new ArrayList<>();
        this.removedNodes = new ArrayList<>();
    }
    public void addRemovedRingEntry(HashRingEntry entry){
        this.removedNodes.add(entry);
    }


    public void addNewRingEntry(HashRingEntry entry){
        this.newNodes.add(entry);
    }

    public void clearHashringEntriesList(){
        this.newNodes.clear();
        this.removedNodes.clear();
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
}
