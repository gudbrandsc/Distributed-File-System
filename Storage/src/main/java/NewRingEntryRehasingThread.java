import Hash.BalancedHashRing;
import Hash.HashException;
import Hash.HashRing;
import Hash.HashRingEntry;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

public class NewRingEntryRehasingThread extends Thread {
    private static HashMap<String, Clientproto.SNReceive> dataStore;
    private static ArrayList<String> filesInSystem;
    private HashRingEntry newNeighbor;
    private BalancedHashRing balancedHashRing;

    public NewRingEntryRehasingThread(HashRingEntry newNeighbor, HashMap<String, Clientproto.SNReceive> dataStore, BalancedHashRing balancedHashRing, ArrayList<String> filesInSystem){
     this.newNeighbor = newNeighbor;
     this.dataStore = new HashMap<>(dataStore);
     this.balancedHashRing = balancedHashRing;
     this.filesInSystem = filesInSystem;
    }


    public void run() {
        System.out.println("Starting to rehash: ");
        System.out.println("Number of chunks stored: " + dataStore.size());

        for(Clientproto.SNReceive data : dataStore.values()){
            if(data.getFileData().getReplicaNum() == 1){
                String hashString = data.getFileData().getFilename() + data.getFileData().getChunkNo();
                try {
                    BigInteger node = balancedHashRing.locate(hashString.getBytes());
                    if(node == newNeighbor.position){
                        System.out.println("Neighbor should store this.");
                        reHashChunk(Clientproto.SNReceive.packetType.STORE, data,1);
                    }else{
                        reHashChunk(Clientproto.SNReceive.packetType.PIPELINE,data,2);
                    }
                } catch (HashException e) {
                    e.printStackTrace();
                }

            }else{
                reHashChunk(Clientproto.SNReceive.packetType.PIPELINE,data,data.getFileData().getReplicaNum());
            }
        }
        CountDownLatch latch = new CountDownLatch(1);

        for (String name : filesInSystem){
            Clientproto.SNReceive broadcastMessage = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.BROADCAST).setFileData(Clientproto.FileData.newBuilder().setFilename(name).build()).build();
            SendBroadCastThread sendBroadCastThread = new SendBroadCastThread(latch, newNeighbor, broadcastMessage );
            sendBroadCastThread.start();
        }
    }

    private void reHashChunk(Clientproto.SNReceive.packetType type, Clientproto.SNReceive data, int replicaNum){
        Clientproto.SNReceive sendChunk = null;
       if(type == Clientproto.SNReceive.packetType.PIPELINE){
           Clientproto.FileData fileData = data.getFileData();
           Clientproto.FileData pipedata = Clientproto.FileData.newBuilder().setChunkNo(fileData.getChunkNo()).setFilename(fileData.getFilename()).setData(fileData.getData()).setNumChunks(fileData.getNumChunks()).setReplicaNum(replicaNum).build();
           sendChunk = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.PIPELINE).setFileData(pipedata).build();
       }else{
           System.out.println("Sending store of: " + data.getFileData().getChunkNo());
           sendChunk = data;
       }
       sendDataToNeighbor(sendChunk, newNeighbor);

    }
    private Clientproto.SNReceive sendDataToNeighbor(Clientproto.SNReceive chunk, HashRingEntry neighbor){
        Socket socket = null;

        try {
            socket = new Socket(neighbor.getIp(), neighbor.getPort());
            InputStream instream = socket.getInputStream();
            OutputStream outstream = socket.getOutputStream();
            chunk.writeDelimitedTo(outstream);
            Clientproto.SNReceive snReceive = Clientproto.SNReceive.parseDelimitedFrom(instream);
            socket.close();
            return snReceive;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

    }
}