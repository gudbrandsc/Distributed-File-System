import Hash.BalancedHashRing;
import Hash.HashException;
import Hash.HashRingEntry;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class OldNeighborRehashThread extends Thread{
    private static HashMap<String, Clientproto.SNReceive> dataStore;
    private BalancedHashRing balancedHashRing;
    private BigInteger position;


    public OldNeighborRehashThread(HashMap<String, Clientproto.SNReceive> dataStore, BalancedHashRing balancedHashRing, BigInteger position){
    this.balancedHashRing = balancedHashRing;
    this.dataStore = dataStore;
    this.position = position;
    }

    public void run() {
        System.out.println("Start rehashing as the old neighbor..");
        HashMap<String, Clientproto.SNReceive> dataStoreCopy = new HashMap<>(dataStore);
        for(Clientproto.SNReceive data : dataStoreCopy.values()){
            if(data.getFileData().getReplicaNum() == 2){
                String hashString = data.getFileData().getFilename() + data.getFileData().getChunkNo();

                try {
                    BigInteger node =  balancedHashRing.locate(hashString.getBytes());
                    //If i should store first chunk now
                    if(node.compareTo(position) == 0){

                        if(replicateChunk(data)){
                            Clientproto.SNReceive firstReplica = rebuildReplica(data,1);
                            String key = firstReplica.getFileData().getFilename() + firstReplica.getFileData().getChunkNo() + firstReplica.getFileData().getReplicaNum();
                            dataStore.put(key, firstReplica);
                        }else{
                            System.out.println("Unable to rehash chunk: " + data.getFileData().getChunkNo() + " successfully");
                        }
                    }

                } catch (HashException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private boolean replicateChunk(Clientproto.SNReceive data){
        HashRingEntry neighbor =  balancedHashRing.getEntry(position).neighbor;
        System.out.println("Trying to store chunk num: " + data.getFileData().getChunkNo() + " replica 2");
        return pipelineChunk(neighbor, data).getSuccess();

    }

    private Clientproto.SNReceive pipelineChunk(HashRingEntry neighbor, Clientproto.SNReceive data){
        Socket neighborSocket = null;

        try {
            neighborSocket = new Socket(neighbor.getIp(), neighbor.getPort());
            InputStream instream = neighborSocket.getInputStream();
            OutputStream outstream = neighborSocket.getOutputStream();
            rebuildReplica(data,2).writeDelimitedTo(outstream);
            Clientproto.SNReceive resp = Clientproto.SNReceive.parseDelimitedFrom(instream);
            neighborSocket.close();
            return resp;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Clientproto.SNReceive rebuildReplica(Clientproto.SNReceive data, int replicanum){
        Clientproto.FileData fileData = data.getFileData();
        Clientproto.FileData newFileData = Clientproto.FileData.newBuilder().setData(fileData.getData()).setChunkNo(fileData.getChunkNo())
                .setNumChunks(fileData.getNumChunks()).setFilename(fileData.getFilename()).setReplicaNum(replicanum).build();



        return Clientproto.SNReceive.newBuilder().setFileData(newFileData).setType(Clientproto.SNReceive.packetType.PIPELINE).setFileExist(true).build();
    }
        //TODO start rehashing as 2
}
