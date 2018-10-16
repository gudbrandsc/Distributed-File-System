import com.google.protobuf.ByteString;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @author gudbrandschistad
 * Manager thread used to store data to the cluster.
 */
public class StoreFileManager extends Thread{
    private static Random randomGenerator;
    private File file;
    private static ArrayList<StorageNodeInfo> storageNodes;
    private String filename;
    private CountDownLatch latch;
    private int coordPort;
    private String coordIp;

    /** Constructor */
    public StoreFileManager(File file,ArrayList<StorageNodeInfo> storageNodes, Random randomGenerator, String filename, CountDownLatch latch, String coordIp, int coordPort){
        this.randomGenerator = randomGenerator;
        this.file = file;
        this.storageNodes = storageNodes;
        this.filename = filename;
        this.latch = latch;
        this.coordIp = coordIp;
        this.coordPort = coordPort;

    }
    /**
     * Run method that divided a file into chunks and send the data using protocol buffer objects
     */
    public void run(){
        byte[] bytearray = readFileToByteArray(file);
        System.out.println("Size of file: " + file.length() + " array size: " + file.length());
        randomGenerator = new Random();
        ArrayList<Clientproto.SNReceive> chunkArray = createFileChunks(bytearray, filename);
        CountDownLatch updateLatch = new CountDownLatch(1);
        //Update system info to get the newest possible list of available storage nodes.
        UpdateSystemInfoThread updateSystemInfoThread = new UpdateSystemInfoThread(storageNodes, updateLatch,coordPort,coordIp);
        updateSystemInfoThread.start();

        try {
            updateLatch.await();
            //For each chunk of data, send it to a random node for load balacing.
            for(Clientproto.SNReceive chunkData : chunkArray ){
                int random = randomGenerator.nextInt(storageNodes.size());
                StorageNodeInfo storageNode = storageNodes.get(random);
                CountDownLatch chunkLatch = new CountDownLatch(1);
                System.out.println("Sending chunk num: " +chunkData.getFileData().getChunkNo() + " to node: " + storageNode.getId());
                DataStorageThread dataStorageThread = new DataStorageThread(storageNode, chunkData, chunkLatch);
                dataStorageThread.start();
                chunkLatch.await();

            }
            //If a fill was successfully stored, inform all other nodes about the the data available in the system.
            broadCastNewFile(filename);
            System.out.println("File was successfully stored");
        } catch (InterruptedException e) {
            System.out.println("Failed to store file.");
            //e.printStackTrace();
        }
        latch.countDown();
    }

    /**
     * Sends a message to all nodes that a new file is available in the cluster*/
    private static void broadCastNewFile(String filename) throws InterruptedException {
        System.out.println("Send broadcast");
        Clientproto.SNReceive broadcastMessage = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.BROADCAST).setSendBroadCast(true).setFileData(Clientproto.FileData.newBuilder().setFilename(filename).build()).build();
        StorageNodeInfo storageNode = getRandomNode();
        Socket socket = null;

        try {
            socket = new Socket(storageNode.getIp(), storageNode.getPort());
            OutputStream outstream = socket.getOutputStream();
            broadcastMessage.writeDelimitedTo(outstream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * This method uses java.io.FileInputStream to read
     * file content into a byte array
     * @param file
     * @return
     */
    private static byte[] readFileToByteArray(File file){
        FileInputStream fis = null;
        byte[] bArray = new byte[(int) file.length()];
        try{
            fis = new FileInputStream(file);
            fis.read(bArray);
            fis.close();

        }catch(IOException ioExp){
            ioExp.printStackTrace();
        }
        return bArray;
    }

    /**
     * Split a bytearray into multible protocol buff objects that will be sent to a storage node.
     */
    private static ArrayList<Clientproto.SNReceive> createFileChunks(byte[] bytearray, String filename) {
        int start = 0;
        int packetNo = 1;
        int packetSize = 0;
        ArrayList<Clientproto.SNReceive> chunkArray = new ArrayList<Clientproto.SNReceive>();
        int numberOfChunks = (int) Math.ceil(bytearray.length / (double) 32);
        for (int i = 0; i <= bytearray.length; i++) {

            if ((((i + 1) % 32) == 0) && (i != 0)) {
                byte[] testArray = Arrays.copyOfRange(bytearray, start, i);
                Clientproto.FileData fileData = Clientproto.FileData.newBuilder().setData(ByteString.copyFrom(testArray)).setChunkNo(packetNo).setFilename(filename).setNumChunks(numberOfChunks).setReplicaNum(1).build();
                Clientproto.SNReceive storeChunk = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.STORE).setFileData(fileData).setFileExist(true).build();
                chunkArray.add(storeChunk);
                start = i ;
                packetNo++;
                packetSize = 0;
            } else if (i == bytearray.length) {
                System.out.println("Created packet with packet number: " + packetNo);
                System.out.println("Packet size: " + packetSize);
                byte[] testArray = Arrays.copyOfRange(bytearray, start, i);
                Clientproto.FileData fileData = Clientproto.FileData.newBuilder().setData(ByteString.copyFrom(testArray)).setChunkNo(packetNo).setFilename(filename).setNumChunks(numberOfChunks).setReplicaNum(1).build();
                Clientproto.SNReceive storeChunk = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.STORE).setFileData(fileData).setFileExist(true).build();
                chunkArray.add(storeChunk);
            } else {
                packetSize++;
            }
        }
        System.out.println("Total number of chunks created: " + numberOfChunks);
        return chunkArray;

    }

    /**
     * Get a random storage node.
     */
    private static StorageNodeInfo getRandomNode(){
        randomGenerator = new Random();
        int random = randomGenerator.nextInt(storageNodes.size());
        return storageNodes.get(random);
    }

}
