public class StorageNodeInfo {
    private String ip;
    private int port;
    private int id;

    public StorageNodeInfo(String ip, int port, int id){
        this.ip = ip;
        this.port = port;
        this.id = id;
    }

    public String getIp() {
        return this.ip;
    }

    public int getPort() {
        return this.port;
    }

    public int getId() {
        return this.id;
    }
}
