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
    private SystemDataStore systemDataStore;
    private HashRingEntry newNeighbor;
    private BalancedHashRing balancedHashRing;

    public NewRingEntryRehasingThread(HashRingEntry newNeighbor, BalancedHashRing balancedHashRing,SystemDataStore systemDataStore) {
     this.newNeighbor = newNeighbor;
     this.balancedHashRing = balancedHashRing;
     this.systemDataStore = systemDataStore;
    }


    public void run() {
        System.out.println("Starting to rehash: ");

        for(Clientproto.SNReceive data : systemDataStore.getDataStoreCopy().values()){
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

        for (String name : systemDataStore.getFilesInSystem()){
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