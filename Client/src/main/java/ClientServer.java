import com.google.protobuf.ByteString;

import java.io.*;
import java.math.RoundingMode;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;


public class ClientServer {
    private static boolean running = true;
    private static ByteString byteString = ByteString.EMPTY;
    private static ArrayList<StorageNodeInfo> storageNodes = new ArrayList<>();
    private static Random randomGenerator;

    //TODO CORDINATOR DOWN
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
            } else if (command.trim().equalsIgnoreCase("store")) {
                storeFile();
            } else if (command.trim().equalsIgnoreCase("System")) {
                getSystemReport();
                printSystemReport();
            } else if (command.trim().equalsIgnoreCase("read")) {
                retrieveFile();
            } else if (command.trim().equalsIgnoreCase("ls")) {
                displayFilesInSystem();
            } else if (command.trim().equalsIgnoreCase("nodeinfo")) {
                getNodeInfo();
            } else if (command.trim().equalsIgnoreCase("Exit")) {
                System.out.println("Bye =)");
                System.exit(1);
            }else{
                System.out.println("Invalid command");

            }

        }
    }

    private static void getNodeInfo(){
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter ip: ");
        String ip = sc.nextLine();
        System.out.println("Enter port: ");
        String port = sc.nextLine();
        boolean exist = false;

        for(StorageNodeInfo node : storageNodes){
            if(node.getIp().equals(ip) && node.getPort() == Integer.parseInt(port)){
                exist = true;
            }
        }

        if(exist){

            Socket socket = null;
            Clientproto.SNReceive message = Clientproto.SNReceive.newBuilder().setType(Clientproto.SNReceive.packetType.SYSTEM).build();
            try {
                socket = new Socket(ip, Integer.parseInt(port));
                OutputStream outstream = socket.getOutputStream();
                InputStream instream = socket.getInputStream();
                message.writeDelimitedTo(outstream);
                Clientproto.SNReceive reply = Clientproto.SNReceive.parseDelimitedFrom(instream);
                System.out.println("-------Files and chunks stored at " + ip + ":" + port + "--------");

                for(String filename: reply.getNodeFilesList()){
                    System.out.println(filename);
                    for(Clientproto.FileData data : reply.getChunkListList()){
                        if(data.getFilename().equals(filename)){
                            System.out.println("\t Chunk num: " + data.getChunkNo() + " of " + data.getNumChunks() + " replica number: " + data.getReplicaNum());
                        }
                    }
                }
                System.out.println("-----------------------------------------");

            } catch (IOException e) {
                e.printStackTrace();
            }

        }else{
            System.out.println("Node does not exist");
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

            if(reply.getNodeFilesList().size() == 0){
                System.out.println("There's no files in the system yet");
            }

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
        System.out.println("Enter file to retrieve from the system: ");
        String filename = sc.nextLine();
        System.out.println("Enter the name you want to store the file as: ");
        String writeFilename = sc.nextLine();
        RetrieveDataManager retrieveDataManager = new RetrieveDataManager(filename, storageNodes, writeFilename);
        retrieveDataManager.start();
    }

    private static void getSystemReport(){
        CountDownLatch latch = new CountDownLatch(1);
        UpdateSystemInfoThread updateSystemInfoThread = new UpdateSystemInfoThread(storageNodes, latch);
        updateSystemInfoThread.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void printSystemReport(){
        double totAvailSpace = 0.0;
        DecimalFormat df = new DecimalFormat("#.00");
        df.setRoundingMode(RoundingMode.FLOOR);


        for(StorageNodeInfo info : storageNodes){
            totAvailSpace += (info.getAvail_space()/1000000000.00);
        }

        System.out.println(" ---------------------------------------------------------------------");
        System.out.println("|                         System Report                              |");
        System.out.println(" ---------------------------------------------------------------------");
        System.out.println("| Space available:  " + df.format(totAvailSpace) + " GB");
        System.out.println("| Available storage nodes: ");
        for(StorageNodeInfo node: storageNodes){
            System.out.println("|\tID: "+ node.getId() + " --> " +node.getIp() + ":" + node.getPort() + " requests handled " + node.getReq_handled());
        }

        System.out.println(" ---------------------------------------------------------------------");
    }



    private static void storeFile() {
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter filename: ");
        String filename = sc.nextLine();
        File file = new File("./Client/src/main/resources/" + filename);
        if(file.exists()) {
            CountDownLatch latch = new CountDownLatch(1);
            StoreFileManager storeFileManager = new StoreFileManager(file, storageNodes, randomGenerator, filename, latch);
            storeFileManager.start();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }else{
            System.out.println("File does not exist. ");
        }
    }



    /**
     * Method that prints all possible commands to the user
     */
    private static void listCommands () {
        System.out.println("-- exit -- Exit the program");
        System.out.println("-- store --  Display all other users.");
        System.out.println("-- read -- Display all received broadcast messages.");
        System.out.println("-- ls --  List all available files");
        System.out.println("-- system -- Display system info ");
        System.out.println("-- nodeinfo --  Get");


    }

    private static StorageNodeInfo getRandomNode(){
        randomGenerator = new Random();
        int random = randomGenerator.nextInt(storageNodes.size());
        return storageNodes.get(random);
    }
}
