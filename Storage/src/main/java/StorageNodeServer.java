import Hash.*;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class StorageNodeServer {
    private static boolean running = true;
    private static volatile int nodeId;
    private static int  port;
    private static int  coordPort;
    private static String  coordIp;
    private static int totAvailableSpace = 0;
    private static int totRequestsHandled = 0;
    private static HashMap<String, StorageNodeInfo> storageNodeInfoHashMap = new HashMap<String, StorageNodeInfo>();
    private static BalancedHashRing balancedHashRing;
    private static volatile HashMap<String, Clientproto.SNReceive> dataStore = new HashMap<>();
    private static volatile ArrayList<String> filesInSystem = new ArrayList<>();


    public static void main(String[] args) throws UnknownHostException {

        if(args.length != 6){
            System.out.println("ERROR: To few arguments");
            System.exit(0);
        } else if((!args[0].equals("-myport")) || (!args[2].equals("-coordIp")) || (!args[4].equals("-coordPort"))){
            System.out.println("Wrong syntax while passing args");
            System.out.println("Expected: -myport **** -coordIp ***** -coordPort ****");
            System.out.println("Found: " + args[0] + " " + args[1] + " " + args[2] + " " + args[3] + " " + args[4] + " " + args[5]);
            System.exit(0);
        }
        port = Integer.parseInt(args[1]);
        coordIp = args[3];
        coordPort = Integer.parseInt(args[5]);

        System.out.println("Starting storage node on port " + port + "...");
        ServerSocket serve = null;
        try {
            serve = new ServerSocket(port);

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed to start server");
            System.exit(1);

        }
        SHA1 sha1 = new SHA1();
        balancedHashRing = new BalancedHashRing(sha1);

        Clientproto.CordResponse reply = sendJoinRequest();
        if(!reply.getCanJoin()){
            System.exit(1);
        }

        nodeId = reply.getNodeId();
        System.out.println("Joined cluster with id: " + nodeId);

        //Create hashring


        readMessages(serve, storageNodeInfoHashMap, totRequestsHandled, totAvailableSpace, nodeId, dataStore, filesInSystem);
        //TODO fix static input
        HearbeatThread hearbeatThread = new HearbeatThread(coordIp, coordPort, "127.0.0.1", port, storageNodeInfoHashMap, balancedHashRing, nodeId, dataStore, filesInSystem);
        hearbeatThread.start();

        for(int i = 0; i < 100; i++){
            System.out.println("------------My data-------------");
          /*  for(Clientproto.SNReceive data: dataStore.values()){
                if(data.getFileData().getReplicaNum() == 1){
                    System.out.println("I have first Chunk " + data.getFileData().getChunkNo());

                }

            }
            for(Clientproto.SNReceive data: dataStore.values()) {
                System.out.println("Chunk: " + data.getFileData().getChunkNo() + " replica: " + data.getFileData().getReplicaNum());
            }*/

          ArrayList<HashRingEntry> hashRingEntries = new ArrayList<>(balancedHashRing.getEntryMap().values());
            for(HashRingEntry entry : hashRingEntries){
                System.out.println(entry.getIp() + ":" + entry.getPort() + " id " +entry.getNodeId() + " Neigborid " + entry.neighbor.getNodeId() + " --> " + entry.getPosition());
            }

            System.out.println("-------------------------");
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * Method that runs in separate thread from main and waits for incoming messages at a port.
     * If a message is recived it creates a new thread to handle it and continues to listen for new messages
     * @param serve ServerSocket object
     * @param filesInSystem
     */
    private static void readMessages(final ServerSocket serve, final HashMap<String, StorageNodeInfo> storageNodeInfos, final int totRequestsHandled, final int totAvailableSpace, int nodeId, HashMap<String, Clientproto.SNReceive> dataStorage, ArrayList<String> filesInSystem){
        final Runnable run = new Runnable() {
            public void run() {
                try {
                    while(running) {
                        Socket sock = serve.accept();
                        //Create thread to handle request

                        MessageListener messageListener = new MessageListener(sock, storageNodeInfos, nodeId, balancedHashRing, dataStore, filesInSystem);
                        messageListener.start();
                    }
                } catch(IOException ioe) {
                    ioe.printStackTrace();
                }
            }
        };
        new Thread(run).start();
    }


    private static Clientproto.CordResponse sendJoinRequest(){
        Socket socket = null;
        Clientproto.CordResponse reply = null;

        try {
            socket = new Socket(coordIp, coordPort);
            InputStream instream = socket.getInputStream();
            OutputStream outstream = socket.getOutputStream();
            Clientproto.CordReceive message = Clientproto.CordReceive.newBuilder().setType(Clientproto.CordReceive.packetType.JOIN).setIp(socket.getInetAddress().getHostAddress()).setPort(port).build();
            message.writeDelimitedTo(outstream);
            reply = Clientproto.CordResponse.parseDelimitedFrom(instream);
            try {
                addAllNodes(reply);
            } catch (HashTopologyException e) {
                e.printStackTrace();
            }
            socket.close();

            return reply;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    private static void addAllNodes(Clientproto.CordResponse reply) throws HashTopologyException {
        TreeMap<BigInteger, Clientproto.NodeInfo> funnymap = new TreeMap<>();

        for( Clientproto.NodeInfo node: reply.getNewNodesList()) {
            ByteString bytes = node.getPosition().getPosition();
            BigInteger position = new BigInteger(bytes.toByteArray());
            funnymap.put(position, node);
        }
        for(Map.Entry<BigInteger,Clientproto.NodeInfo> entry : funnymap.entrySet()) {
            BigInteger key = entry.getKey();
            Clientproto.NodeInfo value = entry.getValue();
            try {
                balancedHashRing.addNodeWithPosition(key,value.getId(),value.getIp(),value.getPort());
            } catch (HashException e) {
                e.printStackTrace();
            }
        }
    }
}
