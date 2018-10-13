import Hash.BalancedHashRing;
import Hash.HashException;
import Hash.HashRingEntry;
import java.io.*;
import java.math.BigInteger;
import java.net.Socket;
import java.net.UnknownHostException;



public class StoreRequestThread extends Thread {
    private Socket socket;
    private Clientproto.SNReceive chunk;
    private BalancedHashRing balancedHashRing;
    private int nodeId;
    private SystemDataStore systemDataStore;

    /** Constructor */
    public StoreRequestThread(Socket socket, Clientproto.SNReceive chunk, BalancedHashRing balancedHashRing, int nodeId,SystemDataStore systemDataStore) {
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

        try {
            BigInteger node = balancedHashRing.locate(hashString.getBytes());
            HashRingEntry entry = balancedHashRing.getEntry(node);
            if(entry.getNodeId() == nodeId){
                Clientproto.SNReceive reply = null;
                String key = chunk.getFileData().getFilename() + chunk.getFileData().getChunkNo() + chunk.getFileData().getReplicaNum();
                if(replicateChunk()){
                    systemDataStore.addDataStore(key, chunk);
                    writeDataToStorage(chunk);
                    reply = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.STORE).setSuccess(true).build();
                }else{
                    System.out.println("Unable to replicate successfully");
                    reply = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.STORE).setSuccess(false).build();
                }
                OutputStream outstream = socket.getOutputStream();
                reply.writeDelimitedTo(outstream);

            }else{
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

    private boolean writeDataToStorage(Clientproto.SNReceive data){

        try {
            String filename = data.getFileData().getFilename() + "-" + data.getFileData().getChunkNo() + "-" + data.getFileData().getReplicaNum();
            File dataFile = new File("./DataStorage" + nodeId + "/" + filename);
            File dataChecksumFile = new File("./DataStorage" + nodeId + "/" + filename + "-checksum");
            byte[] dataToStore = data.getFileData().getData().toByteArray();
            if (dataFile.exists()) {
                dataFile.delete();
            }

            if (dataChecksumFile.exists()) {
                dataChecksumFile.delete();
            }

            dataFile.createNewFile();
            dataChecksumFile.createNewFile();

            FileOutputStream oi = new FileOutputStream(dataFile);
            oi.write(dataToStore);

            FileOutputStream checksumWrite = new FileOutputStream(dataChecksumFile);
            checksumWrite.write(dataToStore);

            System.out.println("File was successfully written");
        } catch (IOException e) {
            System.out.println("Unable to build file..");
            e.printStackTrace();
        }
        return true;
    }

}
