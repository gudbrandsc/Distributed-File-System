import com.google.protobuf.ByteString;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CoordServer {

    private static boolean running = true;
    private volatile static HashMap<String,StorageNodeInfo> storageNodeInfoList = new HashMap<String,StorageNodeInfo>();
    private static AtomicInteger nodeId = new AtomicInteger(1);
    private static int totAvailableSpace = 0;
    private static int totRequestsHandled = 0;


    public static void main(String[] args) {
        System.out.println("Starting coordinator server on port 5001...");

        try {
            ServerSocket serve = new ServerSocket(Integer.parseInt("5001"));
            readMessages(serve, storageNodeInfoList, nodeId);

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed to start server");

        }

    }

    /**
     * Method that runs in separate thread from main and waits for incoming messages at a port.
     * If a message is recived it creates a new thread to handle it and continues to listen for new messages
     * @param serve ServerSocket object
     */
    private static void readMessages(final ServerSocket serve, final HashMap<String, StorageNodeInfo> storageNodeInfos, final AtomicInteger nodeId){
        final Runnable run = new Runnable() {
            public void run() {
                try {
                    while(running) {
                        Socket sock = serve.accept();
                        //Create thread to handle request
                        MessageListener messageListener = new MessageListener(sock, storageNodeInfos, nodeId,totRequestsHandled,totAvailableSpace);
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
