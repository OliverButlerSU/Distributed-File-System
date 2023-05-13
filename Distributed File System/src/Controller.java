import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** A controller which controls the communication between all connected DStores and Clients */
public class Controller {

    /** Controller port */
    private final int cport;

    /** Replication factor for storing values across R DStores */
    private final int replicationFactor;

    /** Timeout in milliseconds for communicating with a DStore and receiving an ACK */
    private final int timeout;

    /** Rebalance period in milliseconds for rebalancing files */
    private final int rebalancePeriod;

    /** Index file state for storing in progress */
    private final String indexStoreInProgress = "STORE_IN_PROGRESS ";

    /** Index file state for removing in progress */
    private final String indexRemoveInProgress = "REMOVE_IN_PROGRESS ";

    /** Controller socket */
    private ServerSocket socket;

    /** List of all connected DStores */
    private final ArrayList<DstoreMessageWriter> dstores = new ArrayList<>();

    /** List of all connected clients */
    private final ArrayList<ClientMessageWriter> clients = new ArrayList<>();

    /** Index storing all files */
    private final Index index = new Index();

    /** Client lock for synchronising file loading, removing and storing */
    private final Object clientLock = new Object();

    public static void main(String[] args) {
        try {
            final int cport = Integer.parseInt(args[0]);
            final int R = Integer.parseInt(args[1]);
            final int timeout = Integer.parseInt(args[2]);
            final int rebalancePeriod = Integer.parseInt(args[3]);

            // launch the controller
            new Controller(cport, R, timeout, rebalancePeriod);
        } catch (Exception e) {
            System.err.println("Error parsing commandline arguments");
        }
    }

    public Controller(int cport, int R, int timeout, int rebalancePeriod) {
        this.cport = cport;
        this.replicationFactor = R;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;

        createSocket();
    }

    /** Creates server socket and listens for any connecting clients of DStores */
    private void createSocket() {
		try {
			//Create a server socket to listen to new messages
			socket = new ServerSocket(cport);
			while (true) {
				try {
					//Accept any new clients and make a new socket for them
					Socket client = socket.accept();

					new Thread(() ->{
						try{
							//Create message reader and writer
							BufferedReader message = new BufferedReader(new InputStreamReader(client.getInputStream()));
							PrintWriter printWriter = new PrintWriter(client.getOutputStream());

							//Read message
							String line = message.readLine();

							//If the client sends a "JOIN" message, create a new dStore, else create a client
							if(line.split(" ")[0].equals(Protocol.JOIN_TOKEN)){
								System.out.println("Creating a new Dstore");
								DstoreMessageWriter dstoreController = new DstoreMessageWriter(client, message, line,printWriter);
								dstores.add(dstoreController);
							} else{
								System.out.println("Creating a new Client");
								ClientMessageWriter clientController = new ClientMessageWriter(client, message, line, printWriter);
								clients.add(clientController);
								new Thread(clientController).start();
							}
						} catch (Exception e){
							System.err.println("Error in creating Thread: " + e);
						}

					}).start();
				} catch(Exception e) {
					System.err.println("Error in accepting socket: " + e);
				}
			}
		} catch(Exception e) {
			System.err.println("Error in creating server socket: " + e);
		}
	}

    /** A class used for handling client messages */
    public class ClientMessageWriter implements MessageReceiverWriter {

        /** Socket connection */
        private final Socket socket;

        /** Used to read messages sent from client */
        private final BufferedReader messageReader;

        /** Used to write messages to client */
        private final PrintWriter messageWriter;

        public ClientMessageWriter(
                Socket socket, BufferedReader messages, String line, PrintWriter messageWriter) {
            this.socket = socket;
            this.messageReader = messages;
            this.messageWriter = messageWriter;
            handleMessage(line);
        }

        /** Reads a message sent by the Client and handles it */
        @Override
        public void run() {
            // Receive messages
            try {
                String line;
                while ((line = messageReader.readLine()) != null) handleMessage(line);
                socket.close();
                clients.remove(this);
                System.out.println("Closing socket");
            } catch (Exception e) {
                System.err.println("error: " + e);
            }
        }

        /**
         * Sends a message to the client
         *
         * @param message message to send
         */
        private void sendClientMessage(String message) {
            System.out.println("Sending message \"" + message + "\" to Client");
            messageWriter.println(message);
            messageWriter.flush();
        }

        /**
         * Handles a message sent by the client
         *
         * @param message Message sent from client
         */
        public void handleMessage(String message) {
            System.out.println("Message: \"" + message + "\" received from Client");

            // Ensures there are enough connected DStores
            if (dstores.size() < replicationFactor) {
                sendClientMessage(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                return;
            }

            String splitMessage = message.split(" ")[0];

            switch (splitMessage) {
                case Protocol.LIST_TOKEN:
                    sendClientMessage(Protocol.LIST_TOKEN + " " + index.getAllFiles());
                    break;
                case Protocol.STORE_TOKEN:
                    storeFile(message);
                    break;
                case Protocol.LOAD_TOKEN:
                    resetCounter(message);
                    loadFile(message);
                    break;
                case Protocol.RELOAD_TOKEN:
                    reloadFile(message);
                    break;
                case Protocol.REMOVE_TOKEN:
                    deleteFile(message);
                    break;
            }
        }

        /**
         * Reloads a file by incrementing the amount of times it has been reloaded
         *
         * @param message message sent by client
         */
        private void reloadFile(String message) {
            String filename = message.split(" ")[1];
            if (!index.containsFilename(filename)) {
                sendClientMessage(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                return;
            }

            // Increment the amount of times accessed
            index.incrementCurrentStoreFiles(filename);
            loadFile(message);
        }

        /**
         * Resets the counter for accessing a file
         *
         * @param message message sent by client
         */
        private void resetCounter(String message) {
            try {
                String filename = message.split(" ")[1];
                index.resetCurrentStoreFile(filename);
            } catch (Exception e) {
                System.err.println("Client message is malformed");
            }
        }

        /**
         * Loads a file from a DStore, sending the port to the user to load from
         *
         * @param message message sent by client
         */
        private void loadFile(String message) {
            try {
                String filename = message.split(" ")[1];
                if (!index.containsFilename(filename)) {
                    sendClientMessage(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    return;
                }

                if (checkForLoadLock(filename)) {
                    return;
                }

                // Get all DStores that store the file
                ArrayList<Integer> ports = index.getPortsWithFile(filename);
                int size = index.getFileSize(filename);
                int currentPort = index.getCurrentStoreFile(filename);

                // If enough reloads have been called that there are no DStores left to connect to
                if (currentPort >= ports.size()) {
                    sendClientMessage(Protocol.ERROR_LOAD_TOKEN);
                    index.resetCurrentStoreFile(filename);
                    return;
                }

                sendClientMessage(Protocol.LOAD_FROM_TOKEN + " " + ports.get(currentPort) + " " + size);
            } catch (Exception e) {
                System.err.println("Error in getting filename");
            }
        }

        /**
         * Checks if the current file being loaded is being stored or removed
         *
         * @param filename name of file
         * @return Boolean
         */
        private boolean checkForLoadLock(String filename) {
            synchronized (clientLock) {
                if (index.currentStateStoring(filename)) {
                    sendClientMessage(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    return true;
                }

                if (index.currentStateRemoving(filename)) {
                    sendClientMessage(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    return true;
                }
                return false;
            }
        }

        /**
         * Stores a file sent by the client over R DStores
         *
         * @param message message sent from client
         */
        private void storeFile(String message) {
            try {
                String filename = message.split(" ")[1];
                int filesize = Integer.parseInt(message.split(" ")[2]);

                if (index.containsFilename(filename)) {
                    sendClientMessage(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                    return;
                }

                if (checkForStoreLock(filename)) {
                    return;
                }

                // Get R DStores
                List<Integer> ports = index.getRDStores(replicationFactor);
                StringBuilder sb = new StringBuilder();
                for (Integer port : ports) {
                    index.addDStoreFile(port, filename);
                    sb.append(" ").append(port);
                }

                sendClientMessage(Protocol.STORE_TO_TOKEN + sb);

				// Checks to see if all DStores send an ACK back
				new Thread(() -> {

					int neededACKs = ports.size();
					AtomicInteger currentACKs = new AtomicInteger();
					long startTime = System.currentTimeMillis();

					// Get all DStores with the port and listen for store ack
					for (DstoreMessageWriter dstore: dstores){
						if(ports.contains(dstore.getPort())){
							new Thread(() ->{
								while(true){
									if(System.currentTimeMillis() - startTime >= timeout){
										System.err.println("Could not connect to DStore: " + dstore.getPort() + " in time");
										break;
									}
									String line;
									try{
										if((line = dstore.messageReader.readLine()) != null){
											if(line.equals(Protocol.STORE_ACK_TOKEN + " " + filename)){
												currentACKs.addAndGet(1);
												index.addDStoreFile(dstore.getPort(), filename);
												break;
											}
										}
									} catch (Exception e) {
										e.printStackTrace();
									}
								}
							}).run();
						}
					}
					if(currentACKs.get() == neededACKs){
						sendClientMessage(Protocol.STORE_COMPLETE_TOKEN);
						index.addFileSizes(filename, filesize);
					} else{
						index.removeFiles(filename);
					}

					index.removeCurrentState(indexStoreInProgress + filename);
				}).start();
			} catch (Exception e){
				System.err.println("Error in getting ports to send to");
				e.printStackTrace();
			}
		}

        /**
         * Checks if the current file being stored is already being stored or removed
         *
         * @param filename name of file
         * @return Boolean
         */
        private boolean checkForStoreLock(String filename) {
            synchronized (clientLock) {
                if (index.currentStateStoring(filename)) {
                    sendClientMessage(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                    return true;
                }

                if (index.currentStateRemoving(filename)) {
                    sendClientMessage(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                    return true;
                }
                index.setCurrentState(indexStoreInProgress + filename);
                return false;
            }
        }

        /**
         * Deletes a file across R DStores
         *
         * @param message message sent by client
         */
        private void deleteFile(String message) {
            try {
                String filename = message.split(" ")[1];
                if (!index.containsFilename(filename)) {
                    sendClientMessage(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    return;
                }

                if (checkForRemoveLock(filename)) {
                    return;
                }

                index.removeFiles(filename);

                // Get all DStores storing file
                ArrayList<Integer> ports = index.getPortsWithFile(filename);
                int neededACKs = ports.size();
                AtomicInteger currentACKs = new AtomicInteger();
                long startTime = System.currentTimeMillis();

				//Listen for a REMOVE_ACK sent back from the dstore
				for (DstoreMessageWriter dstore: dstores){
					if(ports.contains(dstore.getPort())){
						new Thread(() ->{
							dstore.sendDStoreMessage(Protocol.REMOVE_TOKEN + " " + filename);
							while(true){
								//Check for timeout
								if(System.currentTimeMillis() - startTime >= timeout){
									System.err.println("Could not connect to DStore: " + dstore.getPort() + " in time");
									break;
								}

								String line;
								try{
									if((line = dstore.messageReader.readLine()) != null){
										if(line.equals(Protocol.REMOVE_ACK_TOKEN + " " + filename)){
											currentACKs.addAndGet(1);
											break;
										}
									}
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						}).run();
					}
				}
				//If all ACKs were received, complete the remove
				if(currentACKs.get() == neededACKs){
					sendClientMessage(Protocol.REMOVE_COMPLETE_TOKEN);
					index.removeCurrentState(indexRemoveInProgress + filename);
				}
			} catch (Exception e){
				System.err.println("Error in getting ports to send to");
				e.printStackTrace();
			}
		}

        /**
         * Checks if the current file being removed is already being stored or removed
         *
         * @param filename name of file
         * @return Boolean
         */
        private boolean checkForRemoveLock(String filename) {
            synchronized (clientLock) {
                if (index.currentStateStoring(filename)) {
                    sendClientMessage(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    return true;
                }

                if (index.currentStateRemoving(filename)) {
                    sendClientMessage(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    return true;
                }
                index.setCurrentState(indexRemoveInProgress + filename);
                return false;
            }
        }
    }

    /** A class used for handling DStore messages */
    public class DstoreMessageWriter {

        /** Socket connection */
        private final Socket socket;

        /** Used to read messages sent from client */
        private final BufferedReader messageReader;

        /** Used to write messages to client */
        private final PrintWriter messageWriter;

        /** DStore port */
        private int port = 0;

        public DstoreMessageWriter(
                Socket socket, BufferedReader messages, String line, PrintWriter messageWriter) {
            this.socket = socket;
            this.messageReader = messages;
            this.messageWriter = messageWriter;
            try {
                this.port = Integer.parseInt(line.split(" ")[1]);
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }

            handleMessage(line);
        }

        /**
         * Handles a DStore message
         *
         * @param message message sent from DStore
         */
        public void handleMessage(String message) {
            System.out.println("Message: \"" + message + "\" received from DStore");

            String splitMessage = message.split(" ")[0];
            switch (splitMessage) {
                case Protocol.JOIN_TOKEN:
                    sendDStoreMessage(Protocol.LIST_TOKEN);
                    addFileToIndex();
                    break;
            }
        }

        /**
         * Send a message to the DStore
         *
         * @param message message to send
         */
        public void sendDStoreMessage(String message) {
            System.out.println("Sending message \"" + message + "\" to DStore: " + port);
            messageWriter.println(message);
            messageWriter.flush();
        }

        /** Add files sent from LIST request to index */
        private void addFileToIndex() {
            try {
                String message;
                long startTime = System.currentTimeMillis();
                while (true) {
                    if (System.currentTimeMillis() - startTime >= timeout) {
                        // Timeout has occurred
                        return;
                    }
                    if ((message = messageReader.readLine()) != null) {
                        break;
                    }
                }

                String[] splitMessage = message.split(" ");
                ArrayList<String> listOfFiles = new ArrayList<>();

                if (splitMessage.length <= 1) {
                    System.out.println("DStore contains no files");
                    index.addFiles(port, listOfFiles);
                    return;
                }

				listOfFiles.addAll(Arrays.asList(splitMessage).subList(1, splitMessage.length));
                index.addFiles(port, listOfFiles);
            } catch (Exception e) {
                System.err.println("Error in splitting list message");
            }
        }

        /**
         * Gets the DStore port
         *
         * @return port
         */
        public int getPort() {
            return port;
        }
    }
}
