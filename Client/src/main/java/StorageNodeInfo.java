public class StorageNodeInfo {
    private String ip;
    private int port;
    private int id;
    private int avail_space;
    private int req_handled;

    public StorageNodeInfo(String ip, int port, int id, int avail_space, int req_handled){
        this.ip = ip;
        this.port = port;
        this.id = id;
        this.avail_space = avail_space;
        this.req_handled = req_handled;
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

    public int getAvail_space() {
        return avail_space;
    }

    public int getReq_handled() {
        return req_handled;
    }
}
