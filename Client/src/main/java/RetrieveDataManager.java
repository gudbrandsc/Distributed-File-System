import com.google.protobuf.ByteString;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @author Gudbrand Schistad
 * Manager thread for file retrieval. Starts a thread to retrive all chunks for a file, and rebuild the file.
 */
public class RetrieveDataManager extends Thread{
    private String filename;
    private String writeFilename;
    private String coordIp;
    private int coordPort;

    private static ByteString byteString = ByteString.EMPTY;
    private static ArrayList<StorageNodeInfo> storageNodes;
    private static HashMap<Integer, Clientproto.SNReceive> fileChunks;
    private static Random randomGenerator;


    //TODO Have list of storage nodes her and add random. If timeout then send to an other.
    /**Constructor*/
    RetrieveDataManager(String filename, ArrayList<StorageNodeInfo> storageNodes, String writeFilename, String coordIp, int coordPort) {
        this.storageNodes = storageNodes;
        this.filename = filename;
        this.writeFilename = writeFilename;
        this.fileChunks = new HashMap<>();
        this.coordIp = coordIp;
        this.coordPort = coordPort;
    }

    /**
     * Run method that gets all filechunks from different storage nodes
     */
    public void run() {
        System.out.println("Trying to get file for you..");
        //todo if not respons send to new node
        int tryCount = 1;
        boolean success = false;
        Clientproto.SNReceive reply = null;
        CountDownLatch updateStorageLatch = new CountDownLatch(1);
        UpdateSystemInfoThread updateSystemInfoThread = new UpdateSystemInfoThread(storageNodes, updateStorageLatch,coordPort,coordIp );
        updateSystemInfoThread.start();

        try {
            updateStorageLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }



        while(tryCount < 4 && !success){
            reply = getFirstChunk();
            System.out.println("Count: " + tryCount);

            if(reply == null){
                tryCount++;
            }else{
                success = true;
            }
        }

        if(success) {
            if (reply.getFileExist()) {
                int numChunks = reply.getFileData().getNumChunks();
                fileChunks.put(1, reply);
                if (getAllFileChunks(numChunks)) {
                    if (verifyChunkMap()) {
                        buildFile();
                    } else {
                        System.out.println("Unable to retrieve all chunks");
                    }
                } else {
                    System.out.println("Latch timed out");
                }
            } else {
                System.out.println("file does not exist..");
            }
        }else{
            System.out.println("Unable to retrieve first chunk...");
        }
    }

    /**
     * Method to build a new file from the retrieved data. */
    private void buildFile(){
        try {
            byte[] finish = byteString.toByteArray();

            System.out.println(writeFilename);
            File yourFile = new File("./" + writeFilename);
            if (yourFile.exists()) {
                yourFile.delete();
            }
            yourFile.createNewFile(); // if file already exists will do nothing
            FileOutputStream oi = new FileOutputStream("./" + writeFilename);

            oi.write(finish);
            System.out.println("File was successfully written");
        } catch (IOException e) {
            System.out.println("Unable to build file..");
            e.printStackTrace();
        }
    }

    /**
     * Method used to verify that all chunks have been retrieved
     */
    private boolean verifyChunkMap(){
        boolean abort = false;
        int chunkNumber = 1;

        while (!abort && chunkNumber <= fileChunks.size()) {
            if (fileChunks.get(chunkNumber) == null) {
                System.out.println("Found null for chunk " + chunkNumber);
                return false;
            } else {
                byteString = byteString.concat(fileChunks.get(chunkNumber).getFileData().getData());
            }
            chunkNumber++;
        }
        return true;
    }

    /**
     * Starts a RetrieveChunkThread to get each chunk from the cluster.
     */
    private boolean getAllFileChunks(int numChunks){
        CountDownLatch latch = new CountDownLatch(numChunks - 1);

        for (int i = 2; i <= numChunks; i++) {
            fileChunks.put(i, null);
            RetrieveChunkThread retrieveChunkThread = new RetrieveChunkThread(filename, storageNodes, i, latch, fileChunks);
            retrieveChunkThread.start();
        }

        try {
            latch.await();
            return true;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }

    }

    /**
     * Retrieve first chunk from a random storage node
     */
    private Clientproto.SNReceive getFirstChunk(){
        StorageNodeInfo storageNode = getRandomNode();
        Socket socket = null;
        //System.out.println("Trying to get chunk " + 1 + " from node: " + storageNode.getId());

        try {
            socket = new Socket(storageNode.getIp(), storageNode.getPort());
            InputStream instream = socket.getInputStream();
            OutputStream outstream = socket.getOutputStream();
            Clientproto.SNReceive message = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.RETRIEVE).setFileData(Clientproto.FileData.newBuilder().setFilename(filename).setChunkNo(1).setReplicaNum(1).build()).build();
            message.writeDelimitedTo(outstream);

            Clientproto.SNReceive reply = Clientproto.SNReceive.parseDelimitedFrom(instream);

            socket.close();

            return reply;

        } catch (IOException e) {
            System.out.println("Unable to get chunk: " + 1);
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Method that returns a random storage node.
     */
    private StorageNodeInfo getRandomNode(){
        randomGenerator = new Random();
        int random = randomGenerator.nextInt(storageNodes.size());
        return storageNodes.get(random);
    }

}