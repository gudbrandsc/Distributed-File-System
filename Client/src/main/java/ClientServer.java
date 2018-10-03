import com.google.protobuf.ByteString;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CountDownLatch;


public class ClientServer {
    private static boolean running = true;
    private static ByteString byteString = ByteString.EMPTY;
    private static ArrayList<StorageNodeInfo> storageNodes = new ArrayList<>();
    private static int availableSpace = 0;
    private static int requestsHandled = 0;
    private static Random randomGenerator;


    public static void main(String[] args) {
        System.out.println("Starting client on port 5000...");

        try {
            ServerSocket serve = new ServerSocket(Integer.parseInt("5000"));
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed to start server");

        }

        getSystemReport();
        while (running) {
            System.out.println("Enter command: ");
            Scanner scanner = new Scanner(System.in);
            String command = scanner.nextLine();
            if (command.trim().equalsIgnoreCase("help")) {
                listCommands();
            } else if (command.trim().equalsIgnoreCase("Send")) {
                storeFile();
            } else if (command.trim().equalsIgnoreCase("System")) {
                getSystemReport();
                printSystemReport();
            } else if (command.trim().equalsIgnoreCase("read")) {
                //Todo Read from command
                retrieveFile();
            } else if (command.trim().equalsIgnoreCase("ls")) {
                    displayFilesInSystem();
            } else if (command.trim().equalsIgnoreCase("Exit")) {
                System.out.println("Bye =)");
                System.exit(1);
            }
        }
    }

    private static void displayFilesInSystem(){
        Socket socket = null;
        Clientproto.SNReceive message = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.SYSTEM).build();
        try {
            StorageNodeInfo node = getRandomNode();
            socket = new Socket(node.getIp(), node.getPort());
            InputStream instream = socket.getInputStream();
            OutputStream outstream = socket.getOutputStream();
            message.writeDelimitedTo(outstream);
            Clientproto.SNReceive reply = Clientproto.SNReceive.parseDelimitedFrom(instream);
            System.out.println("-------Available files in cluster--------");

            for(String filename: reply.getNodeFilesList()){
                System.out.println(filename);
            }

            System.out.println("-----------------------------------------");

            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void retrieveFile(){
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter filename with path...:");
        String filename = sc.nextLine();
        RetrieveDataManager systemReportResponder = new RetrieveDataManager( filename, storageNodes);
        systemReportResponder.start();
    }

    private static void getSystemReport(){
        Socket socket = null;
        Clientproto.CordResponse reply = null;

        try {
            socket = new Socket("localhost", 5001);
            InputStream instream = socket.getInputStream();
            OutputStream outstream = socket.getOutputStream();

            Clientproto.CordReceive message = Clientproto.CordReceive.newBuilder().setType(Clientproto.CordReceive.packetType.SYSTEM).build();
            message.writeDelimitedTo(outstream);
            reply = Clientproto.CordResponse.parseDelimitedFrom(instream);
            availableSpace = reply.getAvailSpace();
            requestsHandled = reply.getReqHandled();
            storageNodes.clear();
            for(Clientproto.NodeInfo nodeInfo : reply.getAllNodesList()){
                storageNodes.add(new StorageNodeInfo(nodeInfo.getIp(), nodeInfo.getPort(), nodeInfo.getId()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void printSystemReport(){
        System.out.println(" ---------------------------------------------------------------------");
        System.out.println("|                         System Report                              |");
        System.out.println(" ---------------------------------------------------------------------");
        System.out.println("| Space available:  " + availableSpace + " GB");
        System.out.println("| Request handled: " + requestsHandled);
        System.out.println("| Available storage nodes: ");
        for(StorageNodeInfo node: storageNodes){
            System.out.println("|\tID: "+ node.getId() + " --> " +node.getIp() + ":" + node.getPort());
        }
        System.out.println(" ---------------------------------------------------------------------");
    }



    private static void storeFile() {
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter filename with path...:");
        String filename = sc.nextLine();
        File file = new File("/Users/gudbrandschistad/IdeaProjects/P1-gudbrandsc/Client/src/main/java/" + filename);
        byte[] bytearray = readFileToByteArray(file);
        System.out.println("Size of file: " + file.length() + " array size: " + file.length());

        randomGenerator = new Random();
        System.out.println(storageNodes.size() + " node list size");
        ArrayList<Clientproto.SNReceive> chunkArray = createFileChunks(bytearray, filename);
        CountDownLatch latch = new CountDownLatch(chunkArray.size());
        for(Clientproto.SNReceive chunkData : chunkArray ){
            int random = randomGenerator.nextInt(storageNodes.size());
            StorageNodeInfo storageNode = storageNodes.get(random);
            DataStorageThread dataStorageThread = new DataStorageThread(storageNode, chunkData,latch);
            dataStorageThread.start();
        }

        try {
            latch.await();
            broadCastNewFile(filename);
            System.out.println("File was successfully stored");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void broadCastNewFile(String filename){
        Clientproto.SNReceive broadcastMessage = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.BROADCAST).setSendBroadCast(true).setFileData(Clientproto.FileData.newBuilder().setFilename(filename).build()).build();
        CountDownLatch latch = new CountDownLatch(1);
        StorageNodeInfo storageNode = getRandomNode();
        DataStorageThread dataStorageThread = new DataStorageThread(storageNode, broadcastMessage,latch);
        dataStorageThread.start();
    }

    private static ArrayList<Clientproto.SNReceive> createFileChunks(byte[] bytearray, String filename) {
        int start = 0;
        int packetNo = 1;
        int packetSize = 0;
        ArrayList<Clientproto.SNReceive> chunkArray = new ArrayList<Clientproto.SNReceive>();
        int numberOfChunks = (int) Math.ceil(bytearray.length / (double) 32);
        for (int i = 0; i <= bytearray.length; i++) {

            if ((((i + 1) % 32) == 0) && (i != 0)) {
               // System.out.println("Range: " + start +" - " + i);
                byte[] testArray = Arrays.copyOfRange(bytearray, start, i);
                Clientproto.FileData fileData = Clientproto.FileData.newBuilder().setData(ByteString.copyFrom(testArray)).setChunkNo(packetNo).setFilename(filename).setNumChunks(numberOfChunks).setReplicaNum(1).build();
                Clientproto.SNReceive storeChunk = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.STORE).setFileData(fileData).setFileExist(true).build();
                chunkArray.add(storeChunk);
                start = i ;
               // System.out.println("Created packet with packet number: " + packetNo);
                packetNo++;
                packetSize = 0;
            } else if (i == bytearray.length) {
               // System.out.println("Range: " + start +" - " + i);

                System.out.println("Created packet with packet number: " + packetNo);
                System.out.println("Size = " + packetSize);
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
     * This method uses java.io.FileInputStream to read
     * file content into a byte array
     * @param file
     * @return
     */
    private static byte[] readFileToByteArray(File file){
        FileInputStream fis = null;
        // Creating a byte array using the length of the file
        // file.length returns long which is cast to int
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
     * Method that prints all possible commands to the user
     */
    private static void listCommands () {
        System.out.println("-- Broadcast -- Send a message to  all other users.");
        System.out.println("-- Exit -- Exit the program");
        System.out.println("-- List --  Display all other users.");
        System.out.println("-- Read -- Display all received broadcast messages.");
        System.out.println("-- Send -- Send a message to a user.");
    }

    private static StorageNodeInfo getRandomNode(){
        randomGenerator = new Random();
        int random = randomGenerator.nextInt(storageNodes.size());
        return storageNodes.get(random);
    }
}
