import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SystemDataStore {
    private volatile ArrayList<String> filesInSystem;
    private volatile HashMap<String, Clientproto.SNReceive> dataStore;
    private volatile HashMap<String, Clientproto.SNReceive> dataStoreChecksum;
    private volatile int totAvailableSpace;
    private AtomicInteger totRequestsHandled;

    public SystemDataStore(){
        this.filesInSystem = new ArrayList<>();
        this.dataStore = new HashMap<>();
        this.dataStoreChecksum= new HashMap<>();
        this.totAvailableSpace = 1000000000;
        this.totRequestsHandled = new AtomicInteger(0);
    }


    public HashMap<String, Clientproto.SNReceive> getDataStoreCopy() {
        return new HashMap<>(dataStore);
    }


    public int getTotAvailableSpace() {
        return totAvailableSpace;
    }

    public AtomicInteger getTotRequestsHandled() {
        return totRequestsHandled;
    }

    public void addFilesInSystem(String filename) {
        if(!this.filesInSystem.contains(filename)) {
            this.filesInSystem.add(filename);
        }
    }

    public ArrayList<String> getFilesInSystem() {
        return filesInSystem;
    }

    public void addDataStore(String key, Clientproto.SNReceive data){
        this.dataStore.put(key,data);
        this.dataStoreChecksum.put(key,data);
        totAvailableSpace -= data.getFileData().getData().size();
    }

    public boolean filesExist(String filename){
        if(filesInSystem.contains(filename)){
            return true;
        }
        return false;
    }

    public boolean chunkExist(String key){
        if(dataStore.containsKey(key)){
            return true;
        }
        return false;
    }

    public Clientproto.SNReceive getChunkData(String key){
        return dataStore.get(key);
    }
    public Clientproto.SNReceive getChecksumData(String key){
        return dataStore.get(key);
    }

}
