# Project 1
In this project, you will build your own distributed file system (DFS) based on the technologies we’ve studied from Amazon, Google, and others. Your DFS will support multiple storage nodes responsible for managing data. Key features include:

<B>Decentralization:</B> While many distributed file systems require a metadata server to locate files (such as the NameNode in HDFS), each storage node will be able to route client requests directly.
</br><B>Parallel retrievals:</B> large files will be split into multiple chunks. Client applications retrieve these chunks in parallel using threads.
</br><B>Interoperability:</B> the DFS will use Google Protocol Buffers to serialize messages. Do not use Java serialization. This allows other applications to easily implement your wire format.
</br><B>Fault tolerance:</B> your system must be able to detect and withstand two concurrent storage node failures and continue operating normally. It will also be able to recover corrupted files.
Your implementation must be done in Java, and we will test it using the orion cluster here in the CS department. Communication between components must be implemented via sockets (not RMI, RPC or similar technologies) and you may not use any external libraries. The Java Development Kit has everything you need to complete this assignment.

Since this is a graduate-level class, you have leeway on how you design and implement your system. However, you should be able to explain your design decisions. Additionally, you must include the following components:

**Coordinator**
</br>**Storage Node**
</br>**Client**
### Coordinator
The Coordinator’s job is simple: it acts as a gatekeeper to the system so that the administrator can add or remove nodes and monitor the health of the cluster. The Coordinator maintains a canonical routing table, which contains a list of active storage nodes and their positions in the system hash space. Your DFS will implement a zero-hop distributed hash table (DHT) design where each node can locate a file given its name without intermediate routing steps. We will use the SHA-1 hash algorithm.

When a new storage node joins your DFS, the first thing it does is contact the Coordinator. The Coordinator will determine whether or not the node is allowed to enter the system, assigns it a Node ID, and places it in the system hash ring. You get to choose how nodes are positioned within the hash space – remember to justify your algorithm in your design document.

The Coordinator is also responsible for detecting storage node failures and ensuring the system replication level is maintained. In your DFS, every chunk will be replicated twice for a total of 3 duplicate chunks. This means if a node goes down, you can re-route retrievals to a backup copy. You’ll also maintain the replication level by creating more copies in the event of a failure. You will need to design an algorithm for determining replica placement.

The Coordinator should **never** see any files or file names, and does not handle any client storage or retrieval requests. If the Coordinator goes down, file storage and retrieval operations should continue to work. When the Coordinator comes back online, it will request a copy of the last known good hash space and resume its usual operations.

### Storage Node
Storage nodes are responsible for routing client requests as well as storing and retrieving file chunks. When a chunk is stored, it will be checksummed so on-disk corruption can be detected.

Some messages that your storage node could accept (although you are certainly free to design your own):

Store chunk [File name, Chunk Number, Chunk Data]
Get number of chunks [File name]
Get chunk location [File name, Chunk Number]
Retrieve chunk [File name, Chunk Number]
List chunks and file names [No input]
Get copy of current hash space [No input]
Another approach is encoding the chunk number in the file names. Metadata (checksums, chunk numbers) should be stored alongside the files on disk.

The storage nodes will send a heartbeat to the Coordinator periodically to let it know that they are still alive. Every 5 seconds is a good interval for sending these. The heartbeat contains the free space available at the node and the total number of requests processed (storage, retrievals, etc.). If the layout of the hash space has changed, the Coordinator will respond with an updated node list.

On startup: provide a storage directory and the hostname/IP of the Coordinator. Any old files present in the storage directory should be removed. The Coordinator will respond with the current state of the hash space (including the position of the new node).

### Client
The client’s main functions include:

Breaking files into chunks, asking storage nodes where to store them, and then sending them to the appropriate storage node(s).
Note: Once the first chunk has been transferred to its destination storage node, that node will pass the chunk along in a pipeline fashion. The client should not send each chunk 3 times.
If a file already exists, replace it with the new file. If the new file is smaller than the old, you are not required to remove old chunks (but file retrieval should provide the correct data).
Retrieving files in parallel. Each chunk in the file being retrieved will be requested and transferred on a separate thread. Once the chunks are retrieved, the file is reconstructed on the client machine.
The client will also be able to print out a list of active nodes (retrieved from the Coordinator), the total disk space available in the cluster (in GB), and number of requests handled by each node. Given a specific storage node, the client should be able to retrieve a list of files stored there (including the chunk number, e.g., ‘5 of 17’ or similar).

NOTE: Your client must either accept command line arguments or provide its own text-based command entry interface. Recompiling your client to execute different actions is not allowed and will incur a 5 point deduction.
<p>Tips and Resources<br>
Log early, log often! You can use a logging framework or just simple println() calls, but you should print copious log messages. For example, if a StorageNode goes down, the Coordinator should probably print a message acknowledging so. This will help you debug your system and it also makes grading interviews go smoothly.<br>
Use the orion cluster (orion01 – orion12) to test your code in a distributed setting.<br>
These nodes have the Protocol Buffers library installed as well as the protoc compiler. However, you may want to simply bundle the latest version of the library (as a fat jar) with your code instead.<br>
To store your chunk data, use /bigdata/$(whoami), where $(whoami) expands to your user name.<br>
Project Deliverables<br>
This project will be worth 20% of your course grade (20 points). The deliverables include:</p>
<p>[1 pts]: A brief design document. You may use UML diagrams, Vizio, OmniGraffle, etc. This outlines:<br>
Components of your DFS (this includes the components outlined above but might include other items that you think you’ll need)<br>
  
Design decisions (how big the chunks should be, how you will place replicas, etc…)<br>
Messages the components will use to communicate<br>
[4 pts]: Control node implementation:<br>
[1] Maintaining node list and hash space<br>
[1] Detecting node failures<br>
[1] Coordinating replica maintenance<br>
[1] Recovering after a failure<br>
[7 pts]: Storage node implementation:<br>
[1] Routing client requests<br>
[2] Storing chunks and checksums on the local disks<br>
[2] Detecting (and recovering from) file corruption<br>
[1] Handling replica maintenance<br>
[1] Heartbeat messages<br>
[5 pts]: Client implementation:<br>
[2] Storing files (chunk creation, determining appropriate servers)<br>
[2] Retrieving files in parallel<br>
[1] Viewing the node list, available disk space, and requests per node.<br>
[1 pts]: Project retrospective document<br>
[2 pts]: Project checkpoints<br>
Note: your system must be able to support at least 12 active storage nodes, i.e., the entire orion cluster.</p>
