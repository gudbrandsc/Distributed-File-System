import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ChecksumResponder extends Thread {
    private Socket socket;
    private Clientproto.SNReceive chunk;
    private int nodeId;

    public ChecksumResponder(Socket socket, Clientproto.SNReceive chunk, int nodeId) {
        this.socket = socket;
        this.chunk = chunk;
        this.nodeId = nodeId;
    }

    public void run() {
        System.out.println("Sending back my replica of the data...");
        String filename = chunk.getFileData().getFilename() + "-" + chunk.getFileData().getChunkNo() + "-" + chunk.getFileData().getReplicaNum();
        Path fileLocation = Paths.get("./DataStorage" + nodeId + "/" + filename);

        byte[] originalFile = null;

        try {
            originalFile = Files.readAllBytes(fileLocation);
        } catch (IOException e) {
            e.printStackTrace();
        }

        int replicanum = chunk.getFileData().getReplicaNum() - 1;
        Clientproto.FileData fileData = Clientproto.FileData.newBuilder().setData(ByteString.copyFrom(originalFile)).setChunkNo(chunk.getFileData().getChunkNo()).setFilename(chunk.getFileData().getFilename()).setReplicaNum(replicanum).build();
        Clientproto.SNReceive resp = Clientproto.SNReceive.newBuilder().setFileData(fileData).build();

        try {
            resp.writeDelimitedTo(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}