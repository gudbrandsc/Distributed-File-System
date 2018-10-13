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
 * Thread class that is used to print a message recived from an other user, and send a reply.
 */
public class RetrieveDataManager extends Thread{
    private String filename;
    private String writeFilename;

    private static ByteString byteString = ByteString.EMPTY;
    private static ArrayList<StorageNodeInfo> storageNodes;
    private static HashMap<Integer, Clientproto.SNReceive> fileChunks;
    private static Random randomGenerator;


    //TODO Have list of storage nodes her and add random. If timeout then send to an other.
    /**Constructor*/
    RetrieveDataManager(String filename, ArrayList<StorageNodeInfo> storageNodes, String writeFilename) {
        this.storageNodes = storageNodes;
        this.filename = filename;
        this.writeFilename = writeFilename;
        this.fileChunks = new HashMap<>();
    }

    /**
     * Run method that prints the message and sends a reply
     */
    public void run() {
        System.out.println("Trying to get file for you..");
        //todo if not respons send to new node
        int tryCount = 1;
        boolean success = false;
        Clientproto.SNReceive reply = null;
        CountDownLatch updateStorageLatch = new CountDownLatch(1);
        UpdateSystemInfoThread updateSystemInfoThread = new UpdateSystemInfoThread(storageNodes, updateStorageLatch);
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

    private void buildFile(){
        try {
            byte[] finish = byteString.toByteArray();

            System.out.println(writeFilename);
            File yourFile = new File("./Client/src/main/resources/" + writeFilename);
            if (yourFile.exists()) {
                yourFile.delete();
            }
            yourFile.createNewFile(); // if file already exists will do nothing
            FileOutputStream oi = new FileOutputStream("./Client/src/main/resources/" + writeFilename);

            oi.write(finish);
            System.out.println("File was successfully written");
        } catch (IOException e) {
            System.out.println("Unable to build file..");
            e.printStackTrace();
        }
    }

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

    private Clientproto.SNReceive getFirstChunk(){
        //TODO use trycount to get replicanumber
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

    private StorageNodeInfo getRandomNode(){
        randomGenerator = new Random();
        int random = randomGenerator.nextInt(storageNodes.size());
        return storageNodes.get(random);
    }

}
