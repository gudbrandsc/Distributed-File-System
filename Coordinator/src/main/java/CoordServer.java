import Hash.BalancedHashRing;
import Hash.HashRingEntry;
import Hash.SHA1;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CoordServer {

    private static boolean running = true;
    private volatile static HashMap<String, StorageNode> storageNodeInfoList = new HashMap<String, StorageNode>();
    private static AtomicInteger nodeId = new AtomicInteger(0);
    private static int totAvailableSpace = 0;
    private static int totRequestsHandled = 0;
    private static BalancedHashRing balancedHashRing;


    public static void main(String[] args) {
        System.out.println("Starting coordinator server on port 5001...");

        try {
            ServerSocket serve = new ServerSocket(Integer.parseInt("5001"));
            readMessages(serve, storageNodeInfoList, nodeId);

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed to start server");

        }
        SHA1 sha1 = new SHA1();
        balancedHashRing = new BalancedHashRing(sha1);

        for(int i = 0; i < 100; i++){
            System.out.println("------------My ring-------------");

            ArrayList<HashRingEntry> hashRingEntries = new ArrayList<>(balancedHashRing.getEntryMap().values());
            for(HashRingEntry entry : hashRingEntries){
                System.out.println(entry.getIp() + ":" + entry.getPort() + " id " +entry.getNodeId() + " Neigborid " + entry.getNeighbor().getNodeId() + " --> " + entry.getPosition());
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
     */
    private static void readMessages(final ServerSocket serve, final HashMap<String, StorageNode> storageNodeInfos, final AtomicInteger nodeId){
        final Runnable run = new Runnable() {
            public void run() {
                try {
                    while(running) {
                        Socket sock = serve.accept();
                        //Create thread to handle request
                        MessageListener messageListener = new MessageListener(sock, storageNodeInfos, nodeId,totRequestsHandled,totAvailableSpace, balancedHashRing);
                        messageListener.start();
                    }
                } catch(IOException ioe) {
                    ioe.printStackTrace();
                }
            }
        };
        new Thread(run).start();
    }
}
