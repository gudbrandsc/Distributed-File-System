import Hash.BalancedHashRing;
import Hash.HashRingEntry;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;


public class PipelineRequestThread extends Thread {
    private Socket socket;
    private Clientproto.SNReceive chunk;
    private BalancedHashRing balancedHashRing;
    private int nodeId;
    private SystemDataStore systemDataStore;

    /** Constructor */
    public PipelineRequestThread(Socket socket, Clientproto.SNReceive chunk, BalancedHashRing balancedHashRing, int nodeId,SystemDataStore systemDataStore) {
        this.socket = socket;
        this.chunk = chunk;
        this.balancedHashRing = balancedHashRing;
        this.nodeId = nodeId;
        this.systemDataStore = systemDataStore;


    }

    /**
     * Method that sends GET requests to check if a node is alive.
     * If user service is a master then it will check all secondaries and frontend's.
     * If user service is a secondary it will check if the master is alive.
     * Sends heartbeats every 5 seconds
     */
    public void run() {
        int replicaNumber = chunk.getFileData().getReplicaNum();
        Clientproto.SNReceive resp = null;
        String key = chunk.getFileData().getFilename() + chunk.getFileData().getChunkNo()+chunk.getFileData().getReplicaNum();
        System.out.println("Got request to store chunk: " + chunk.getFileData().getChunkNo() + " replica " + chunk.getFileData().getReplicaNum());
        if(replicaNumber != 3) {
            Clientproto.FileData fileData = chunk.getFileData();
            Clientproto.FileData newFileData = Clientproto.FileData.newBuilder().setData(fileData.getData()).setChunkNo(fileData.getChunkNo())
                    .setNumChunks(fileData.getNumChunks()).setFilename(fileData.getFilename()).setReplicaNum(3).build();
            Clientproto.SNReceive newChunk = Clientproto.SNReceive.newBuilder().setFileData(newFileData).setType(Clientproto.SNReceive.packetType.PIPELINE).build();
            HashRingEntry neighbor = balancedHashRing.getEntryById(nodeId).neighbor;

            resp = pipelineChunk(newChunk, neighbor);

            if (resp == null) {
                System.out.println("Unable to pipeline replica 3");
                resp = Clientproto.SNReceive.newBuilder().setSuccess(false).build();
            }else {
                systemDataStore.addDataStore(key,chunk);
                writeDataToStorage(chunk);

            }

        }else {
            resp = Clientproto.SNReceive.newBuilder().setSuccess(true).build();
            systemDataStore.addDataStore(key,chunk);
            writeDataToStorage(chunk);



        }

        OutputStream outstream = null;
        try {
            outstream = socket.getOutputStream();
            resp.writeDelimitedTo(outstream);
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private Clientproto.SNReceive pipelineChunk(Clientproto.SNReceive chunk, HashRingEntry neighbor){
        Socket socket = null;
        System.out.println("Sending replica 3 chunk num:"+ chunk.getFileData().getChunkNo() + " to " + neighbor.getNodeId());

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

    private boolean writeDataToStorage(Clientproto.SNReceive data){
        try {
            String filename = data.getFileData().getFilename() + "-" + data.getFileData().getChunkNo() + "-" + data.getFileData().getReplicaNum();
            File dataFile = new File("./DataStorage"+nodeId+"/" + filename);
            File dataChecksumFile = new File("./DataStorage"+nodeId+"/" + filename + "-checksum");

            if (dataFile.exists()) {
                dataFile.delete();
            }
            if (dataChecksumFile.exists()) {
                dataChecksumFile.delete();
            }
            dataFile.createNewFile();
            dataChecksumFile.createNewFile();

            FileOutputStream oi = new FileOutputStream(dataFile);
            oi.write(data.getFileData().getData().toByteArray());

            FileOutputStream checksumWrite = new FileOutputStream(dataChecksumFile);
            checksumWrite.write(data.getFileData().getData().toByteArray());

            System.out.println("File was successfully written");
        } catch (IOException e) {
            System.out.println("Unable to build file..");
            e.printStackTrace();
        }
        return true;
    }


}
