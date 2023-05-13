import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

/** Stores and serves all files received from clients */
public class Dstore {

    /** DStore port */
    private final int port;

    /** Controller port */
    private final int cport;

    /** Timeout in milliseconds */
    private final int timeout;

    /** Folder location for storing files */
    private final String fileFolder;

    /** Filename -> FileSize */
    private final HashMap<String, Integer> files = new HashMap<>();

    /** Socket for communicating with controller */
    private Socket controllerSocket;

    /** DStore socket */
    private ServerSocket serverSocket;

    /** Address of controller, set to localhost */
    private final String controllerAddress = "127.0.0.1";

    /** Used to send messages to Controller */
    private PrintWriter controllerSocketWriter;

    /** Used to receive messages from Controller */
    private BufferedReader controllerSocketReader;

    public static void main(String[] args) {
        try {
            final int port = Integer.parseInt(args[0]);
            final int cport = Integer.parseInt(args[1]);
            final int timeout = Integer.parseInt(args[2]);
            final String fileFolder = args[3];

            // launch the DStore
            new Dstore(port, cport, timeout, fileFolder);
        } catch (Exception e) {
            System.err.println("Error parsing import");
        }
    }

    public Dstore(int port, int cport, int timeout, String fileFolder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.fileFolder = fileFolder;

        removeAllFiles();
        // addFolderFilesToHashMap();
        createControllerSocket();
        createClientReceiverSocket();
    }

    /** Removes all files originally set in the file folder */
    private void removeAllFiles() {
        // Create folder
        File folder = new File(fileFolder);
        if (!folder.exists()) {
            System.out.println("Creating folder");
            folder.mkdir();
        }

        // Delete each file
        File[] files = folder.listFiles();
        for (File file : files) {
            file.delete();
        }
    }

    /** Adds all files located in the directory to a hash map */
    private void addFolderFilesToHashMap() {
        // Create folder
        File folder = new File(fileFolder);
        if (!folder.exists()) {
            System.out.println("Creating folder");
            folder.mkdir();
        }

        // Add each file to hashmap
        File[] files = folder.listFiles();
        for (File file : files) {
            if (file.isFile()) {
                this.files.put(file.getName(), (int) file.length());
            }
        }
    }

    /** Creates a controller socket */
    private void createControllerSocket() {
        long startTime = getCurrentTime();

        // Connect to controller socket assuming it has not been 1000 seconds
        while (controllerSocket == null) {
            if (checkForTimeout(startTime)) {
                System.exit(1);
                return;
            }
            try {
                controllerSocket = new Socket(controllerAddress, cport);
                controllerSocketWriter = new PrintWriter(controllerSocket.getOutputStream());
                controllerSocketReader = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
                System.out.println("Connected to controller server");
            } catch (Exception e) {
                // Do nothing
            }
        }

        // Send join message
        String joinMessage = Protocol.JOIN_TOKEN + " " + port;
        sendControllerMessage(joinMessage);

		// Create communicator thread
		new Thread(() ->{
			try{
				String line;
				while(true){
					//Listen to messages sent from controller
					if((line = controllerSocketReader.readLine()) != null){
						handleControllerMessage(line);
					}
				}
			} catch (Exception e){
				System.err.println(e.getMessage());
			}
		}).start();
	}

	/** Creates a client receiver socket */
	private void createClientReceiverSocket(){
		new Thread(() ->{
			try{
				serverSocket = new ServerSocket(port);

				while(true){
					//Create a new socket
					Socket client = serverSocket.accept();

					//Create a new client and start thread
					ClientDStore clientDStore = new ClientDStore(client);
					new Thread(clientDStore).start();
				}
			} catch (Exception e){
				System.err.println(e.getMessage());
			}
		}).start();
	}

    /**
     * Handles a Controller message
     *
     * @param message message from Controller
     */
    private void handleControllerMessage(String message) {
        System.out.println("Received message \"" + message + "\" from Controller");

        String[] splitMessage = message.split(" ");
        switch (splitMessage[0]) {
            case Protocol.LIST_TOKEN:
                sendControllerMessage(listFiles());
                break;
            case Protocol.REMOVE_TOKEN:
                removeFile(splitMessage[1]);
                break;
            case Protocol.REBALANCE_TOKEN:
                // TODO: Rebalance files
                break;
        }
    }

    /**
     * Creates a LIST_TOKEN message for listing all files in the DStore
     *
     * @return list of all files
     */
    private String listFiles() {
        StringBuilder message = new StringBuilder(Protocol.LIST_TOKEN);
        for (String filename : files.keySet()) {
            message.append(" " + filename);
        }
        return message.toString();
    }

    /**
     * Sends a message to the controller
     *
     * @param message message to send
     */
    private void sendControllerMessage(String message) {
        System.out.println("Sending message \"" + message + "\" to Controller");
        controllerSocketWriter.println(message);
        controllerSocketWriter.flush();
    }

    /**
     * Checks if a timeout has occurred
     *
     * @param startTime start time for the timeout
     * @return
     */
    private boolean checkForTimeout(long startTime) {
        if (getCurrentTime() - startTime >= timeout) {
            System.err.println("Error in communicating with socket, timeout has occurred");
            return true;
        }
        return false;
    }

    /**
     * Removes a file from the DStore
     *
     * @param filename name of the file
     */
    private void removeFile(String filename) {
        // If the file does not exist, send an error
        if (!files.containsKey(filename)) {
            sendControllerMessage(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
            return;
        }

        // Delete file
        File file = new File(fileFolder + File.separator + filename);
        file.delete();
        files.remove(filename);
        sendControllerMessage(Protocol.REMOVE_ACK_TOKEN + " " + filename);
    }

    /**
     * Gets the current time
     *
     * @return UNIX EPOCH time in long form
     */
    public long getCurrentTime() {
        return System.currentTimeMillis();
    }

    /** A class used for handling messages sent from the Client to the DStore */
    public class ClientDStore implements MessageReceiverWriter {

        /** Socket connection */
        private final Socket socket;

        /** Used to read messages sent from client */
        private final BufferedReader messageReader;

        /** Used to write messages to client */
        private final PrintWriter messageWriter;

        public ClientDStore(Socket socket) throws IOException {
            this.socket = socket;
            this.messageReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.messageWriter = new PrintWriter(socket.getOutputStream());
        }

        /** Listens to messages sent from the client */
        @Override
        public void run() {
            // Receive messages
            try {
                String line;
                while ((line = messageReader.readLine()) != null) handleMessage(line);
                socket.close();
                System.out.println("Closing socket");
            } catch (Exception e) {
                System.err.println("Error: " + e);
            }
        }

        /**
         * Handles a message from the client
         *
         * @param message message sent from client
         */
        public synchronized void handleMessage(String message) {
            System.out.println("Message: \"" + message + "\" received from Client");

            String[] splitMessage = message.split(" ");
            switch (splitMessage[0]) {
                case Protocol.STORE_TOKEN:
                    storeFile(splitMessage[1], splitMessage[2]);
                    break;
                case Protocol.LOAD_DATA_TOKEN:
                    loadFile(splitMessage[1]);
                    break;
            }
        }

        /**
         * Stores a file
         *
         * @param filename name of file
         * @param filesize size of file
         */
        private void storeFile(String filename, String filesize) {
            files.put(filename, Integer.parseInt(filesize));
            try {
                // Send an ACK token
                sendClientMessage(Protocol.ACK_TOKEN);

                // Receive the file and write it
                long startTime = getCurrentTime();
                while (true) {
                    if (checkForTimeout(startTime)) {
                        return;
                    }
                    try {
                        byte[] b = new byte[Integer.parseInt(filesize)];
                        InputStream is = socket.getInputStream();
                        FileOutputStream fr =
                                new FileOutputStream(fileFolder + File.separator + filename);
                        is.read(b, 0, b.length);
                        fr.write(b, 0, b.length);
                        fr.close();
                        break;
                    } catch (Exception e) {
                        System.err.println("Failed to store file to DStore");
                    }
                }

                sendControllerMessage(Protocol.STORE_ACK_TOKEN + " " + filename);
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        /**
         * Loads a file to send to client
         *
         * @param filename name of file
         */
        private void loadFile(String filename) {
            try {
                // Gets the file
                File file = new File(fileFolder + File.separator + filename);
                if ((!file.exists())) {
                    socket.close();
                    return;
                }

                // Get the file content and send
                byte[] b = new byte[files.get(filename)];
                FileInputStream fr = new FileInputStream(fileFolder + File.separator + filename);
                fr.read(b, 0, b.length);
                OutputStream os = socket.getOutputStream();
                os.write(b, 0, b.length);
                fr.close();

            } catch (Exception e) {
                System.out.println(e.getMessage());
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
    }
}
