import com.google.protobuf.ByteString;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @author Gudbrand Schistad
 * Thread class that is used to print a message recived from an other user, and send a reply.
 */
public class RetrieveChunkThread extends Thread{
    private static ArrayList<StorageNodeInfo> storageNodes;
    private String filename;
    private int chunkNumber;
    private HashMap<Integer, Clientproto.SNReceive> fileChunks;
    private CountDownLatch latch;
    private static Random randomGenerator;


    //TODO Have list of storage nodes her and add random. If timeout then send to an other.
    /**Constructor*/
    RetrieveChunkThread(String filename, ArrayList<StorageNodeInfo> storageNodes, int chunkNumber, CountDownLatch latch,HashMap<Integer, Clientproto.SNReceive> fileChunks) {
        this.storageNodes = storageNodes;
        this.filename = filename;
        this.chunkNumber = chunkNumber;
        this.latch = latch;
        this.fileChunks = fileChunks;
    }

    /**
     * Run method that prints the message and sends a reply
     */
    public void run() {
        int tryCount = 1;


        Clientproto.SNReceive reply = null;
        boolean success = false;
        while(!success && tryCount < 4){
            reply = sendReceiveRequest(tryCount);
            if(reply == null){
                tryCount++;
            }else{
                success = true;
            }
        }

        if(success){
            System.out.println("Adding chunk: " +chunkNumber+ " to map");
            fileChunks.put(chunkNumber, reply);
        }
        latch.countDown();
    }

    private Clientproto.SNReceive sendReceiveRequest(int replicaNum){
        //TODO use trycount to get replicanumber
        StorageNodeInfo storageNode = getRandomNode();
        Socket socket = null;
       // System.out.println("Trying to get chunk " + chunkNumber +" replica num: " + replicaNum + " from node: " +storageNode.getId());

        try {
            socket = new Socket(storageNode.getIp(), storageNode.getPort());
            InputStream instream = socket.getInputStream();
            OutputStream outstream = socket.getOutputStream();
            Clientproto.SNReceive message = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.RETRIEVE).setFileData(Clientproto.FileData.newBuilder().setFilename(filename).setChunkNo(chunkNumber).setReplicaNum(replicaNum).build()).build();
            message.writeDelimitedTo(outstream);

            Clientproto.SNReceive reply = Clientproto.SNReceive.parseDelimitedFrom(instream);
            socket.close();
            System.out.println("Got chunk " + chunkNumber +" replica num: " + replicaNum + " from node: " +storageNode.getId());

            return reply;

        } catch (IOException e) {
            System.out.println("Unable to get chunk: " + chunkNumber + " From node: " +storageNode.getId());
            //e.printStackTrace();
            return null;
        }
    }

    private StorageNodeInfo getRandomNode(){
        randomGenerator = new Random();
        int random = randomGenerator.nextInt(storageNodes.size());
        return storageNodes.get(random);
    }

}
