
import Hash.BalancedHashRing;
import Hash.HashException;
import Hash.HashRingEntry;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.util.HashMap;

public class PredecessorRehashThread extends Thread{
    private static HashMap<String, Clientproto.SNReceive> dataStore;
    private BalancedHashRing balancedHashRing;
    private BigInteger position;


    public PredecessorRehashThread(HashMap<String, Clientproto.SNReceive> dataStore, BalancedHashRing balancedHashRing, BigInteger position){
        this.balancedHashRing = balancedHashRing;
        this.dataStore = dataStore;
        this.position = position;
    }

    public void run() {
        System.out.println("Start rehashing as the old predecessor..");
        for(Clientproto.SNReceive data : dataStore.values()) {
            if (data.getFileData().getReplicaNum() < 3) {
                int replicaNumber = data.getFileData().getReplicaNum() + 1;
                Clientproto.SNReceive piplineData = rebuildReplica(data, replicaNumber);
                replicateChunk(piplineData);

            }
        }

    }

    private boolean replicateChunk(Clientproto.SNReceive data){
        HashRingEntry neighbor =  balancedHashRing.getEntry(position).neighbor;
        System.out.println("Trying to store chunk num: " + data.getFileData().getChunkNo() + " replica " + data.getFileData().getReplicaNum());
        return pipelineChunk(neighbor, data).getSuccess();

    }

    private Clientproto.SNReceive pipelineChunk(HashRingEntry neighbor, Clientproto.SNReceive data){
        Socket neighborSocket = null;

        try {
            neighborSocket = new Socket(neighbor.getIp(), neighbor.getPort());
            InputStream instream = neighborSocket.getInputStream();
            OutputStream outstream = neighborSocket.getOutputStream();
            data.writeDelimitedTo(outstream);
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
}
