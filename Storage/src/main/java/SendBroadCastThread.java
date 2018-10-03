import Hash.HashRingEntry;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;


public class SendBroadCastThread extends Thread {
    private CountDownLatch latch;
    private HashRingEntry entry;
    private Clientproto.SNReceive broadcastMessage;

    /** Constructor */
    public SendBroadCastThread(CountDownLatch latch, HashRingEntry entry, Clientproto.SNReceive broadcastMessage) {
        this.latch = latch;
        this.entry = entry;
        this.broadcastMessage = broadcastMessage;
    }

    /**
     * Method that sends GET requests to check if a node is alive.
     * If user service is a master then it will check all secondaries and frontend's.
     * If user service is a secondary it will check if the master is alive.
     * Sends heartbeats every 5 seconds
     */
    public void run() {
        Socket socket = null;

        try {
            socket = new Socket(entry.getIp(), entry.getPort());
            InputStream instream = socket.getInputStream();
            OutputStream outstream = socket.getOutputStream();
            broadcastMessage.writeDelimitedTo(outstream);
            Clientproto.SNReceive reply = Clientproto.SNReceive.parseDelimitedFrom(instream);
            socket.close();
            latch.countDown();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
