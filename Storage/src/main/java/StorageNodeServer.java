import Hash.*;
import com.google.protobuf.ByteString;

import java.io.File;
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
    private static String myIp;
    private static int  coordPort;
    private static String coordIp;
    private static HashMap<String, StorageNodeInfo> storageNodeInfoHashMap = new HashMap<String, StorageNodeInfo>();
    private static BalancedHashRing balancedHashRing;
    private static SystemDataStore systemDataStore;

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
        InetAddress ip = InetAddress.getLocalHost();
        myIp = ip.getHostAddress();

        Clientproto.CordResponse reply = sendJoinRequest();

        if(!reply.getCanJoin()){
            System.exit(1);
        }

        nodeId = reply.getNodeId();
        System.out.println("Joined cluster with id: " + nodeId);
        systemDataStore = new SystemDataStore();
        createStorageDir();
        readMessages(serve, storageNodeInfoHashMap, nodeId, systemDataStore);

        HearbeatThread hearbeatThread = new HearbeatThread(coordIp, coordPort, myIp, port, storageNodeInfoHashMap,
                balancedHashRing, nodeId, systemDataStore);
        hearbeatThread.start();

        for(int i = 0; i < 100; i++){
            System.out.println("------------My data-------------");
            for(Clientproto.SNReceive data: systemDataStore.getDataStoreCopy().values()){
                if(data.getFileData().getReplicaNum() == 1){
                    System.out.println("I have first Chunk " + data.getFileData().getChunkNo());

                }

            }
            for(Clientproto.SNReceive data: systemDataStore.getDataStoreCopy().values()) {
                System.out.println("Chunk: " + data.getFileData().getChunkNo() + " replica: " + data.getFileData().getReplicaNum());
            }
/*
          ArrayList<HashRingEntry> hashRingEntries = new ArrayList<>(balancedHashRing.getEntryMap().values());
            for(HashRingEntry entry : hashRingEntries){
                System.out.println(entry.getIp() + ":" + entry.getPort() + " id " +entry.getNodeId() + " Neigborid " + entry.neighbor.getNodeId() + " --> " + entry.getPosition());
            }*/

            System.out.println("-------------------------");
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    private static boolean createStorageDir(){
        System.out.println("Trying to make dir");
        File theDir = new File("./DataStorage" + nodeId);
        if (!theDir.exists()) {
            System.out.println("creating directory: " + theDir.getName());
            boolean result = false;

            try{
                theDir.mkdir();
                result = true;
            }
            catch(SecurityException se){
                System.out.println("Unable to create storage directory");
            }
            if(result) {
                System.out.println("DIR created");
            }
            return result;
        }
        System.out.println("Dir exist");
        return true;
    }


    private static void readMessages(final ServerSocket serve, final HashMap<String, StorageNodeInfo> storageNodeInfos, final int nodeId, final SystemDataStore systemDataStore){
        final Runnable run = new Runnable() {
            public void run() {
                try {
                    while(running) {
                        Socket sock = serve.accept();
                        //Create thread to handle request
                        MessageListener messageListener = new MessageListener(sock, storageNodeInfos, nodeId, balancedHashRing, systemDataStore);
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
            Clientproto.CordReceive message = Clientproto.CordReceive.newBuilder().setType(Clientproto.CordReceive.packetType.JOIN).setIp(myIp).setPort(port).build();
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
        TreeMap<BigInteger, Clientproto.NodeInfo> tempMap = new TreeMap<>();

        for( Clientproto.NodeInfo node: reply.getNewNodesList()) {
            ByteString bytes = node.getPosition().getPosition();
            BigInteger position = new BigInteger(bytes.toByteArray());
            tempMap.put(position, node);
        }
        for(Map.Entry<BigInteger,Clientproto.NodeInfo> entry : tempMap.entrySet()) {
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
