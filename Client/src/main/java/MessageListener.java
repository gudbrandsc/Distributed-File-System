import java.io.IOException;
import java.net.Socket;
import java.util.List;

/**
 * @author Gudbrand Schistad
 * Thread class that is used to print a message recived from an other user, and send a reply.
 */
public class MessageListener extends Thread{
    private Socket socket;

    /**Constructor*/
    MessageListener(Socket socket) {
        this.socket = socket;
    }

    /**
     * Run method that prints the message and sends a reply
     */
    public void run() {
        Clientproto.ClientReceiveData receiveData = null;


        try {
            receiveData = Clientproto.ClientReceiveData.parseDelimitedFrom(socket.getInputStream());

        } catch (IOException e) {
            e.printStackTrace();
        }
        Clientproto.ClientReceiveData.packetType type = receiveData.getType();


        if(receiveData!=null){
            if (type == Clientproto.ClientReceiveData.packetType.DATA) {
                //TODO add logic for reading.
            }
            /*

            Chatproto.Reply reply = Chatproto.Reply.newBuilder()
                    .setStatus(200)
                    .setMessage("OK")
                    .build();
            try {
                reply.writeDelimitedTo(socket.getOutputStream());
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            */
        }
    }
}
