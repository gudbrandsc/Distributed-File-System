import Hash.BalancedHashRing;
import Hash.HashException;
import Hash.HashRingEntry;
import Hash.SHA1;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;


public class RetrieveRequestThread extends Thread {
    private Socket socket;
    private Clientproto.SNReceive chunk;
    private BalancedHashRing balancedHashRing;
    private int nodeId;
    private HashMap<String, Clientproto.SNReceive> dataStorage;
    private ArrayList<String> filesInSystem;

    /** Constructor */
    public RetrieveRequestThread(Socket socket, Clientproto.SNReceive chunk, BalancedHashRing balancedHashRing, int nodeId,HashMap<String, Clientproto.SNReceive> dataStorage, ArrayList<String> filesInSystem
    ) {
        this.socket = socket;
        this.chunk = chunk;
        this.balancedHashRing = balancedHashRing;
        this.nodeId = nodeId;
        this.dataStorage = dataStorage;
        this.filesInSystem = filesInSystem;

    }

    /**
     * Method that sends GET requests to check if a node is alive.
     * If user service is a master then it will check all secondaries and frontend's.
     * If user service is a secondary it will check if the master is alive.
     * Sends heartbeats every 5 seconds
     */
    public void run() {
        String hashString = chunk.getFileData().getFilename() + chunk.getFileData().getChunkNo();
        if(filesInSystem.contains(chunk.getFileData().getFilename())){

            try {
                BigInteger node = balancedHashRing.locate(hashString.getBytes());
                HashRingEntry entry = balancedHashRing.getEntry(node);

                for (int i = 2; i <= chunk.getFileData().getReplicaNum(); i++){
                    entry = entry.neighbor;
                }


                if(entry.getNodeId() == nodeId){
                    System.out.println("I have chunk: " + chunk.getFileData().getChunkNo());
                    //Store it
                    String key = chunk.getFileData().getFilename() + chunk.getFileData().getChunkNo()+chunk.getFileData().getReplicaNum();
                    System.out.println("The key is "  + key);
                    System.out.println("map size: " + dataStorage.size());
                    OutputStream outstream = socket.getOutputStream();
                    Clientproto.SNReceive reply = null;


                    if(dataStorage.containsKey(key)){
                        reply = dataStorage.get(key);
                        System.out.println("reply object: " + reply.getFileData().getChunkNo());
                        reply.writeDelimitedTo(outstream);
                        socket.close();
                    }else{
                        System.out.println("For some reason i dont have the file after all");
                    }

                }else{
                    //TODO if unable to open socket then return snsend with success = false;
                    System.out.println("Data is stored at node: " + entry.getNodeId());
                    Socket nodeSocket = new Socket(entry.getIp(), entry.getPort());
                    InputStream instream = nodeSocket.getInputStream();
                    OutputStream outstream = nodeSocket.getOutputStream();
                    chunk.writeDelimitedTo(outstream);
                    Clientproto.SNReceive reply = Clientproto.SNReceive.parseDelimitedFrom(instream);
                    nodeSocket.close();



                    outstream = socket.getOutputStream();
                    reply.writeDelimitedTo(outstream);
                    socket.close();
                }
            } catch (HashException e) {
                e.printStackTrace();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            Clientproto.SNReceive reply = Clientproto.SNReceive.newBuilder().setFileExist(false).build();
            System.out.println("Failed to find file");
            try {
                OutputStream outstream = socket.getOutputStream();
                reply.writeDelimitedTo(outstream);
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }
}
