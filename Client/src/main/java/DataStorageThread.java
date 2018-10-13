import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author Gudbrand Schistad
 * Thread class that is used to print a message recived from an other user, and send a reply.
 */
public class DataStorageThread extends Thread{
    private StorageNodeInfo storageNode;
    private Clientproto.SNReceive chunk;
    private static CountDownLatch latch;


    //TODO Have list of storage nodes her and add random. If timeout then send to an other.
    /**Constructor*/
    DataStorageThread(StorageNodeInfo storageNode, Clientproto.SNReceive chunk, CountDownLatch latch) {
        this.storageNode = storageNode;
        this.chunk = chunk;
        this.latch = latch;
    }

    /**
     * Run method that prints the message and sends a reply
     */
    public void run() {
        Socket socket = null;
        Clientproto.SNReceive reply = null;

        try {
            socket = new Socket(storageNode.getIp(), storageNode.getPort());
            InputStream instream = socket.getInputStream();
            OutputStream outstream = socket.getOutputStream();
            chunk.writeDelimitedTo(outstream);
            reply = Clientproto.SNReceive.parseDelimitedFrom(instream);
            if(reply == null){
                System.out.println("Failed to store chunk: " + chunk.getFileData().getChunkNo());

            } else if(!reply.getSuccess() ){
                System.out.println("Failed to store chunk: " + chunk.getFileData().getChunkNo());
            }
            //TODO if failed to pipeline inform client
        } catch (IOException e) {
            System.out.println("lol");
           // e.printStackTrace();
        }
        latch.countDown();
    }
}
