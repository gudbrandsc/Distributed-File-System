import Hash.*;
import com.google.protobuf.ByteString;

import java.io.*;
import java.math.BigInteger;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;


public class RetrieveRequestThread extends Thread {
    private Socket socket;
    private Clientproto.SNReceive chunk;
    private BalancedHashRing balancedHashRing;
    private int nodeId;
    private SystemDataStore systemDataStore;

    /** Constructor */
    public RetrieveRequestThread(Socket socket, Clientproto.SNReceive chunk, BalancedHashRing balancedHashRing, int nodeId,SystemDataStore systemDataStore) {
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
        String hashString = chunk.getFileData().getFilename() + chunk.getFileData().getChunkNo();
        String filename = chunk.getFileData().getFilename() + "-" + chunk.getFileData().getChunkNo()+ "-" + chunk.getFileData().getReplicaNum();
        File file = new File("./DataStorage" + nodeId + "/" + filename);

        if(systemDataStore.filesExist(chunk.getFileData().getFilename())){
            try {
                BigInteger node = balancedHashRing.locate(hashString.getBytes());
                HashRingEntry entry = balancedHashRing.getEntry(node);

                for (int i = 2; i <= chunk.getFileData().getReplicaNum(); i++){
                    entry = entry.neighbor;
                }

                if(entry.getNodeId() == nodeId){
                    String key = chunk.getFileData().getFilename() + chunk.getFileData().getChunkNo()+chunk.getFileData().getReplicaNum();
                    OutputStream outstream = socket.getOutputStream();
                    Clientproto.SNReceive reply = null;
                    if(file.exists()){
                        if(validateWithChecksum(chunk)) {
                            String fileToRead = chunk.getFileData().getFilename() + "-" + chunk.getFileData().getChunkNo() + "-" + chunk.getFileData().getReplicaNum();
                            byte[] chunkData = getChunkData(fileToRead);
                            reply = buildResponse(systemDataStore.getChunkData(key), chunkData);
                            reply.writeDelimitedTo(outstream);
                        }
                    } else{
                        Clientproto.SNReceive resp = Clientproto.SNReceive.newBuilder().setSuccess(false).build();
                        resp.writeDelimitedTo(outstream);
                        System.out.println("For some reason i don't have the file after all");
                    }
                    socket.close();

                }else{
                    //TODO if unable to open socket then return snsend with success = false;
                    System.out.println("Data is stored at node: " + entry.getNodeId());
                    Socket nodeSocket = new Socket(entry.getIp(), entry.getPort());
                    InputStream instream = nodeSocket.getInputStream();
                    OutputStream outstream = nodeSocket.getOutputStream();
                    chunk.writeDelimitedTo(outstream);
                    Clientproto.SNReceive reply = Clientproto.SNReceive.parseDelimitedFrom(instream);
                    nodeSocket.close();

                    outstream = socket.getOutputStream();
                    reply.writeDelimitedTo(outstream);
                    socket.close();
                }
            } catch (HashException e) {
                e.printStackTrace();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            Clientproto.SNReceive reply = Clientproto.SNReceive.newBuilder().setFileExist(false).build();
            System.out.println("Failed to find file");
            try {
                OutputStream outstream = socket.getOutputStream();
                reply.writeDelimitedTo(outstream);
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean validateWithChecksum(Clientproto.SNReceive data){
        System.out.println("Validating with checksum");
        SHA1 sha1 = new SHA1();
        BigInteger value1 = null;
        BigInteger value2 = null;
        String filename = data.getFileData().getFilename() + "-" + data.getFileData().getChunkNo() + "-" + data.getFileData().getReplicaNum();
        Path fileLocation = Paths.get("./DataStorage" + nodeId + "/" + filename);
        Path checksumFileLocation = Paths.get("./DataStorage" + nodeId + "/" + filename + "-checksum");

        try {
            byte[] originalFile = Files.readAllBytes(fileLocation);
            byte[] checksumFile = Files.readAllBytes(checksumFileLocation);
            value1 = sha1.hash(originalFile);
            value2 = sha1.hash(checksumFile);
        } catch (HashException | IOException e) {
            e.printStackTrace();
        }

        if(value1.compareTo(value2) == 0){
            return true;
        } else {
            System.out.println("Checksum failed.");
            fixFileCorruption(data);
            return true;
        }

    }

    private Clientproto.SNReceive buildResponse(Clientproto.SNReceive chunk, byte[] data ){
        Clientproto.FileData fileData = chunk.getFileData();
        Clientproto.FileData newFileData = Clientproto.FileData.newBuilder().setData(ByteString.copyFrom(data)).setChunkNo(fileData.getChunkNo())
                .setNumChunks(fileData.getNumChunks()).setFilename(fileData.getFilename()).setReplicaNum(chunk.getFileData().getReplicaNum()).build();

        return Clientproto.SNReceive.newBuilder().setFileData(newFileData).setType(Clientproto.SNReceive.packetType.PIPELINE).setFileExist(true).setSuccess(true).build();
    }

    private byte[] getChunkData(String key){

        Path fileLocation = Paths.get("./DataStorage" + nodeId + "/" + key);

        try {
            return Files.readAllBytes(fileLocation);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void fixFileCorruption(Clientproto.SNReceive data){
        System.out.println("Fixing file corruption...");
        HashRingEntry neighbor = balancedHashRing.getEntryById(nodeId).neighbor;
        Socket socket = null;
        int replicaNumber = data.getFileData().getReplicaNum() + 1;
        Clientproto.SNReceive reply = null;
        Clientproto.FileData fileData = Clientproto.FileData.newBuilder().setFilename(data.getFileData().getFilename()).setChunkNo(data.getFileData().getChunkNo()).setReplicaNum(replicaNumber).build();
        Clientproto.SNReceive message = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.CHECKSUM).setFileData(fileData).build();

        try {
            socket = new Socket(neighbor.getIp(), neighbor.getPort());
            OutputStream outstream = socket.getOutputStream();
            InputStream instream = socket.getInputStream();
            message.writeDelimitedTo(outstream);
            reply = Clientproto.SNReceive.parseDelimitedFrom(instream);
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        writeDataToFile(reply);
        validateWithChecksum(reply);
    }

    private void writeDataToFile(Clientproto.SNReceive data){
        System.out.println("Writing fixed data to file...");
        try {
            String filename = data.getFileData().getFilename() + "-" + data.getFileData().getChunkNo() + "-" + data.getFileData().getReplicaNum();
            File dataFile = new File("./DataStorage" + nodeId + "/" + filename);
            byte[] dataToStore = data.getFileData().getData().toByteArray();

            if (dataFile.exists()) {
                dataFile.delete();
            }

            dataFile.createNewFile();
            FileOutputStream oi = new FileOutputStream(dataFile);
            oi.write(dataToStore);

        } catch (IOException e) {
            System.out.println("Unable to build file..");
            e.printStackTrace();
        }
    }
}
