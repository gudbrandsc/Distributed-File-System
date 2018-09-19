import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;


public class ClientServer {
    private static boolean running = true;
    private static ByteString byteString = ByteString.EMPTY;
    private static HashMap<Integer, StorageNodeInfo> storageNodes = new HashMap<Integer, StorageNodeInfo>();
    private static int availableSpace = 0;
    private static int requestsHandled = 0;


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
                sendMessage();
            } else if (command.trim().equalsIgnoreCase("System")) {
                getSystemReport();
                printSystemReport();
            } else if (command.trim().equalsIgnoreCase("read")) {
            } else if (command.trim().equalsIgnoreCase("Exit")) {
                System.out.println("Bye =)");
                System.exit(1);
            }
        }
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
            for(Clientproto.NodeInfo nodeInfo : reply.getStorageNodesList()){
                storageNodes.put(nodeInfo.getId(), new StorageNodeInfo(nodeInfo.getIp(),nodeInfo.getPort(),nodeInfo.getId()));
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
        for(int key: storageNodes.keySet()){
            StorageNodeInfo node = storageNodes.get(key);
            System.out.println("|\tID: "+ node.getId() + " --> " +node.getIp() + ":" + node.getPort());
        }
        System.out.println(" ---------------------------------------------------------------------");
    }



    private static void sendMessage() {
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter filename with path...:");
        String filename = sc.nextLine();
        File file = new File("/Users/gudbrandschistad/IdeaProjects/CS677-project1/Client/src/main/java/" + filename);
        // Get length of file in bytes
        long fileSizeInBytes = file.length();
        // Convert the bytes to Kilobytes (1 KB = 1024 Bytes)
        long fileSizeInKB = fileSizeInBytes / 1024;
        // Convert the KB to MegaBytes (1 MB = 1024 KBytes)
        long fileSizeInMB = fileSizeInKB / 1024;

        byte[] bytearray = readFileToByteArray(file);
        System.out.println("Size of file: " + file.length() + " array size: " + file.length());
        createFileChunks(bytearray, filename);

    }

    private static void createFileChunks(byte[] bytearray, String filename) {
        int start = 0;
        int packetNo = 1;
        int packetSize = 0;
        List<Clientproto.SNReceive> superArray = new ArrayList<Clientproto.SNReceive>();
        for (int i = 0; i <= bytearray.length; i++) {

            if ((((i + 1) % 32) == 0) && (i != 0)) {
                System.out.println("Range: " + start +" - " + i);
                byte[] testArray = Arrays.copyOfRange(bytearray, start, i);
                Clientproto.SNReceive storeChunk = Clientproto.SNReceive.newBuilder().setData(ByteString.copyFrom(testArray)).setChunkNo(packetNo).setIsLast(false).setFilename(filename).build();
                superArray.add(storeChunk);
                start = i ;
               // System.out.println("Created packet with packet number: " + packetNo);
                packetNo++;
                packetSize = 0;
            } else if (i == bytearray.length) {
                System.out.println("Range: " + start +" - " + i);

                System.out.println("Created packet with packet number: " + packetNo);
                System.out.println("Size = " + packetSize);
                System.out.println("Packet size: " + packetSize);
                byte[] testArray = Arrays.copyOfRange(bytearray, start, i);
                Clientproto.SNReceive storeChunk = Clientproto.SNReceive.newBuilder().setData(ByteString.copyFrom(testArray)).setChunkNo(packetNo).setIsLast(true).setFilename(filename).build();
                superArray.add(storeChunk);
            } else {
                packetSize++;
            }
        }

        boolean isLast = false;
        int chunkNumber = 1;

        while (!isLast) {
            Clientproto.SNReceive packet = getByteStringNumber(chunkNumber, superArray);
            byteString = byteString.concat(packet.getData());
            if(packet.getIsLast()){
                isLast = true;
                byte[] finish = byteString.toByteArray();
                System.out.println("Writing to a file : out" + packet.getFilename());
                try {
                    FileOutputStream oi = new FileOutputStream("/Users/gudbrandschistad/IdeaProjects/CS677-project1/Client/src/main/java/out" + packet.getFilename());
                    try {
                        oi.write(finish);
                        System.out.println("Done writing");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
            chunkNumber++;
        }
    }

    private static Clientproto.SNReceive getByteStringNumber(int chunckNumber, List<Clientproto.SNReceive> superArray) {
        for (Clientproto.SNReceive packet : superArray) {
            if (packet.getChunkNo() == chunckNumber) {
                return packet;
            }
        }
        System.out.println("Unable to find: " + chunckNumber);
        return null;
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
}
