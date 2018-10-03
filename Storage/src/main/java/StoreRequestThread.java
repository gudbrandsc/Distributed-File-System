


import Hash.BalancedHashRing;
import Hash.HashException;
import Hash.HashRingEntry;
import Hash.SHA1;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;


public class StoreRequestThread extends Thread {
    private Socket socket;
    private Clientproto.SNReceive chunk;
    private BalancedHashRing balancedHashRing;
    private int nodeId;
    private HashMap<String, Clientproto.SNReceive> dataStorage;
    private ArrayList<String> filesInSystem;

    /** Constructor */
    public StoreRequestThread(Socket socket, Clientproto.SNReceive chunk, BalancedHashRing balancedHashRing, int nodeId,HashMap<String, Clientproto.SNReceive> dataStorage,ArrayList<String> filesInSystem) {
        this.socket = socket;
        this.chunk = chunk;
        this.balancedHashRing = balancedHashRing;
        this.nodeId = nodeId;
        this.dataStorage = dataStorage;
        this.filesInSystem = filesInSystem;

    }

    /**
     * Method that sends GET requests to check if a node is alive.
     * If user service is a master then it will check all secondaries and frontend's.
     * If user service is a secondary it will check if the master is alive.
     * Sends heartbeats every 5 seconds
     */
    public void run() {
        String hashString = chunk.getFileData().getFilename() + chunk.getFileData().getChunkNo();

        try {
            BigInteger node = balancedHashRing.locate(hashString.getBytes());
            HashRingEntry entry = balancedHashRing.getEntry(node);
            if(entry.getNodeId() == nodeId){
                Clientproto.SNReceive reply = null;
                String key = chunk.getFileData().getFilename() + chunk.getFileData().getChunkNo() + chunk.getFileData().getReplicaNum();
                if(replicateChunk()){
                    dataStorage.put(key, chunk);
                    reply = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.STORE).setSuccess(true).build();
                }else{
                    System.out.println("Unable to replicate successfully");
                    reply = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.STORE).setSuccess(false).build();
                }
                OutputStream outstream = socket.getOutputStream();
                reply.writeDelimitedTo(outstream);

            }else{
                System.out.println("Data should be store on node: " + entry.getNodeId() + " location node says --> " + node);
                Socket nodeSocket = new Socket(entry.getIp(), entry.getPort());
                InputStream instream = nodeSocket.getInputStream();
                OutputStream outstream = nodeSocket.getOutputStream();
                chunk.writeDelimitedTo(outstream);
                Clientproto.SNReceive reply = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.STORE).build();
                reply = Clientproto.SNReceive.parseDelimitedFrom(instream);
                outstream = socket.getOutputStream();
                reply.writeDelimitedTo(outstream);
            }
        } catch (HashException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Clientproto.SNReceive reply = Clientproto.SNReceive.newBuilder().setTypeValue(0).build();
        try {
            reply.writeDelimitedTo(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private boolean replicateChunk(){
       HashRingEntry neighbor =  balancedHashRing.getEntryById(nodeId).neighbor;
        System.out.println("Trying to store chunk num: " + chunk.getFileData().getChunkNo() + " replica 2");
       return pipelineChunk(neighbor).getSuccess();

    }

    private Clientproto.SNReceive pipelineChunk(HashRingEntry neighbor){
        Socket neighborSocket = null;

        try {
            neighborSocket = new Socket(neighbor.getIp(), neighbor.getPort());
            InputStream instream = neighborSocket.getInputStream();
            OutputStream outstream = neighborSocket.getOutputStream();
            getSecondReplica().writeDelimitedTo(outstream);
            Clientproto.SNReceive resp = Clientproto.SNReceive.parseDelimitedFrom(instream);
            neighborSocket.close();
            return resp;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Clientproto.SNReceive getSecondReplica(){
        Clientproto.FileData fileData = chunk.getFileData();
        Clientproto.FileData newFileData = Clientproto.FileData.newBuilder().setData(fileData.getData()).setChunkNo(fileData.getChunkNo())
                .setNumChunks(fileData.getNumChunks()).setFilename(fileData.getFilename()).setReplicaNum(2).build();
        return Clientproto.SNReceive.newBuilder().setFileData(newFileData).setType(Clientproto.SNReceive.packetType.PIPELINE).build();
    }

}
