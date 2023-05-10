package Coursework;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class Dstore {

	private final int port;
	private final int cport;
	private final int timeout;
	private final String fileFolder;

	//Filename -> Size
	private final HashMap<String, Integer> files = new HashMap<>();
	private Socket controllerSocket;
	private ServerSocket serverSocket;
	private final String controllerAddress = "127.0.0.1";

	private PrintWriter controllerSocketWriter;
	private BufferedReader controllerSocketReader;

	public static void main(String[] args) {

		try{
//			final int port = Integer.parseInt(args[0]);
//			final int cport; = Integer.parseInt(args[1]);
//			final int timeout = Integer.parseInt(args[2]);
//			final String fileFolder = args[3];

			final int port = 4000;
			final int cport = 12345;
			final int timeout = 1000;
			final String fileFolder = "DStoreFiles";

			// launch the controller
//			new Dstore(port, cport, timeout, fileFolder+port);
//			new Dstore(port+1, cport, timeout, fileFolder+(port+1));

			for (int i = 0; i < 2; i++) {
				int finalI = i;
				new Thread(() -> new Dstore(port+finalI, cport, timeout, fileFolder+(port+finalI))).start();
			}
		} catch (Exception e){
			System.err.println("Error parsing import");
		}
	}

	public Dstore(int port, int cport, int timeout, String fileFolder){
		this.port = port;
		this.cport = cport;
		this.timeout = timeout;
		this.fileFolder = fileFolder;

		addFolderFilesToHashMap();
		createControllerSocket();
		createClientReceiverSocket();
	}

	/**
	 * Adds all files located in the directory to a hash map
	 */
	private void addFolderFilesToHashMap(){
		//Create folder
		File folder = new File(fileFolder);
		if(!folder.exists()){
			System.out.println("Creating folder");
			folder.mkdir();
		}

		//Add each file to hashmap
		File[] files = folder.listFiles();
		for(File file: files){
			if(file.isFile()){
				this.files.put(file.getName(), (int)file.length());
			}
		}
	}

	/**
	 * Creates a controller socket
	 */
	private void createControllerSocket(){
		long startTime = getCurrentTime();

		//Connect to controller socket assuming it has not been 1000 seconds
		while(controllerSocket == null){
			if(checkForTimeout(startTime)){
				System.exit(1);
				return;
			}
			try{
				controllerSocket = new Socket(controllerAddress, cport);
				controllerSocketWriter = new PrintWriter(controllerSocket.getOutputStream());
				controllerSocketReader = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
				System.out.println("Connected to controller server");
			} catch (Exception e){
				//System.err.println(e.getMessage());
			}

		}
		//Send join message
		String joinMessage = Protocol.JOIN_TOKEN + " " + port;
		sendControllerMessage(joinMessage);

		//Create communicator thread
		new Thread(() ->{
			try{
				String line;
				while(true){
					if((line = controllerSocketReader.readLine()) != null){
						handleControllerMessage(line);
					}
				}
			} catch (Exception e){
				System.err.println(e.getMessage());
			}
		}).start();
	}

	/**
	 * Creates a client receiver socket
	 */
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
	 * Handles a controller message
	 * @param message message from controller
	 */
	private void handleControllerMessage(String message){
		System.out.println("Received message \"" + message + "\" from controller");

		String[] splitMessage = message.split(" ");
		switch(splitMessage[0]){
			case Protocol.LIST_TOKEN:
				sendControllerMessage(listFiles());
				break;
			case Protocol.REMOVE_TOKEN:
				removeFile(splitMessage[1]);
				break;
			case Protocol.REBALANCE_TOKEN:
				//TODO: Rebalance files
				break;
			default:
				//TODO: Output Error
		}
	}

	/**
	 * Creates a LIST_TOKEN message for listing all files in the DStore
	 * @return message
	 */
	private String listFiles(){
		StringBuilder message = new StringBuilder(Protocol.LIST_TOKEN);
		for(String filename : files.keySet()){
			message.append(" " + filename);
		}
		return message.toString();
	}

	/**
	 * Sends a message to the controller
	 * @param message message to send
	 */
	private void sendControllerMessage(String message){
		System.out.println("Sending message \"" + message + "\" to Controller");
		controllerSocketWriter.println(message);
		controllerSocketWriter.flush();
	}

	/**
	 * Checks if a timeout has occurred
	 * @param startTime start time for the timeout
	 * @return
	 */
	private boolean checkForTimeout(long startTime){
		if(getCurrentTime() - startTime >= timeout){
			System.err.println("Error in communicating with socket, timeout has occurred");
			return true;
		}
		return false;
	}

	/**
	 * Removes a file from the DStore
	 * @param filename name of the file
	 */

	private void removeFile(String filename){
		//If the file does not exist, send an error
		if(!files.containsKey(filename)){
			sendControllerMessage(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
			return;
		}

		//Delete file
		File file = new File(fileFolder + File.separator + filename);
		file.delete();
		files.remove(filename);
		sendControllerMessage(Protocol.REMOVE_ACK_TOKEN + " " + filename);
	}

	/**
	 * Gets the current time
	 * @return UNIX EPOCH time in long form
	 */
	public long getCurrentTime(){
		return System.currentTimeMillis();
	}

	public class ClientDStore implements MessageReceiverWriter {

		private final Socket socket;
		private final BufferedReader messageReader;
		private final PrintWriter messageWriter;

		public ClientDStore(Socket socket) throws IOException {
			this.socket = socket;
			this.messageReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			this.messageWriter = new PrintWriter(socket.getOutputStream());
		}

		@Override
		public void run() {
			//Receive messages
			try {
				String line;
				while((line = messageReader.readLine()) != null)
					handleMessage(line);
				socket.close();
				System.out.println("Closing socket");
			} catch(Exception e) {
				System.err.println("Error: " + e);
			}
		}

		public synchronized void handleMessage(String message){
			System.out.println("Message: \"" + message + "\" received from Client");

			String[] splitMessage = message.split(" ");
			switch(splitMessage[0]){
				case Protocol.STORE_TOKEN:
					storeFile(splitMessage[1],splitMessage[2]);
					break;
				case Protocol.LOAD_DATA_TOKEN:
					loadFile(splitMessage[1]);
					break;
				case Protocol.REBALANCE_STORE_TOKEN:
					//TODO: Rebalance stuff
					break;
				default:
					//TODO: Output error
					break;
			}

		}

		private void storeFile(String filename, String filesize){
			files.put(filename, Integer.parseInt(filesize));
			try{
				//Send an acknowledge token
				sendClientMessage(Protocol.ACK_TOKEN);

				//Receive the file and write it
				String line;
				long startTime = getCurrentTime();
				while(true){
					if(checkForTimeout(startTime)){
						return;
					}
					if ((line = messageReader.readLine()) != null){
						//Write file
						FileWriter fileWriter = new FileWriter(fileFolder + File.separator +  filename);
						System.out.println("Writing file to folder");
						fileWriter.write(line);
						fileWriter.close();
						break;
					}
				}

				sendControllerMessage(Protocol.STORE_ACK_TOKEN + " " + filename);
			} catch (Exception e) {
				System.err.println(e.getMessage());
			}
		}

		private void loadFile(String filename){
			try{
				File file = new File(fileFolder + File.separator + filename);
				if((!file.exists())){
					socket.close();
					return;
				}

				//Read file content
				FileReader fileReader = new FileReader(fileFolder + File.separator + filename);
				char[] fileContent = new char[files.get(filename)];
				fileReader.read(fileContent);
				fileReader.close();

				//Send client file
				sendClientMessage(String.copyValueOf(fileContent));
			} catch (Exception e){
				System.out.println(e.getMessage());
			}
		}

		private void sendClientMessage(String message){
			System.out.println("Sending message \"" + message + "\" to client");
			messageWriter.println(message);
			messageWriter.flush();
		}
	}
}
