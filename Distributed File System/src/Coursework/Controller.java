package Coursework;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Controller {

	private final int cport;
	private final int replicationFactor;
	private final int timeout;
	private final int rebalancePeriod;

	private final String indexStoreInProgress = "STORE_IN_PROGRESS ";
	private final String indexStoreComplete = "STORE_COMPLETE ";
	private final String indexRemoveInProgress = "REMOVE_IN_PROGRESS ";
	private final String indexRemoveComplete = "REMOVE COMPLETE ";

	private ServerSocket socket;
	private ArrayList<DstoreMessageWriter> dstores = new ArrayList<>();
	private ArrayList<ClientMessageWriter> clients = new ArrayList<>();
	private Index index = new Index();

	private Object clientLock = new Object();

	public static void main(String[] args){

		try{
			final int cport = Integer.parseInt(args[0]);
			final int R = Integer.parseInt(args[1]);
			final int timeout = Integer.parseInt(args[2]);
			final int rebalancePeriod = Integer.parseInt(args[3]);

//			final int cport = 12345;
//			final int R = 2;
//			final int timeout = 1000;
//			final int rebalancePeriod = 10000;

			// launch the controller
			new Controller(cport, R, timeout, rebalancePeriod);
		} catch (Exception e){
			System.err.println("Error parsing commandline arguments");
		}
	}

	public Controller(int cport, int R, int timeout, int rebalancePeriod){
		this.cport = cport;
		this.replicationFactor = R;
		this.timeout = timeout;
		this.rebalancePeriod = rebalancePeriod;

		createSocket();
	}

	private void createSocket(){
		try {
			//Create a server socket to listen to new messages
			socket = new ServerSocket(cport);
			while (true) {
				try {

					//Accept any new clients and make a new socket for them
					Socket client = socket.accept();

					//Get the message sent by the client
					BufferedReader message = new BufferedReader(new InputStreamReader(client.getInputStream()));
					PrintWriter printWriter = new PrintWriter(client.getOutputStream());

					String line = message.readLine();
					//If the client sends a "JOIN" message, create a new dStore, else create a client
					if(line.split(" ")[0].equals(Protocol.JOIN_TOKEN)){
						System.out.println("Creating a new Dstore");
						DstoreMessageWriter dstoreController = new DstoreMessageWriter(client, message, line,printWriter);
						dstores.add(dstoreController);
					} else{
						System.out.println("Creating a new client");
						ClientMessageWriter clientController = new ClientMessageWriter(client, message, line, printWriter);
						clients.add(clientController);
						new Thread(clientController).start();
					}
				} catch(Exception e) {
					System.err.println("Error in creating thread: " + e);
				}
			}
		} catch(Exception e) {
			System.err.println("Error in creating socket: " + e);
		}
	}

	public class ClientMessageWriter implements MessageReceiverWriter {

		private Socket socket;
		private BufferedReader messageReader;
		private PrintWriter messageWriter;

		public ClientMessageWriter(Socket socket, BufferedReader messages, String line, PrintWriter messageWriter){
			this.socket = socket;
			this.messageReader = messages;
			this.messageWriter = messageWriter;
			handleMessage(line);
		}

		@Override
		public void run() {
			//Receive messages
			try {
				String line;
				while((line = messageReader.readLine()) != null)
					handleMessage(line);
				socket.close();
				clients.remove(this);
				System.out.println("Closing socket");
			} catch(Exception e) {
				System.err.println("error: " + e);
			}
		}

		public void handleMessage(String message){
			System.out.println("Message: \"" + message + "\" received from Client");

			if(dstores.size() < replicationFactor){
				sendClientMessage(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
				return;
			}

			String splitMessage = message.split(" ")[0];

			switch(splitMessage){
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

		private void sendClientMessage(String message){
			System.out.println("Sending message \"" + message + "\" to Client");
			messageWriter.println(message);
			messageWriter.flush();
		}

		private void reloadFile(String message){
			String filename = message.split(" ")[1];
			index.incrementCurrentStoreFiles(filename);
			loadFile(message);
		}

		private void resetCounter(String message){
			try{
				String filename = message.split(" ")[1];
				index.resetCurrentStoreFile(filename);
			} catch (Exception e){
				//TODO: ERROR
			}
		}

		private void loadFile(String message){
			try{
				String filename = message.split(" ")[1];
				if(!index.containsFilename(filename)){
					sendClientMessage(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
					return;
				}

				if(checkForLoadLock(filename)){
					return;
				}

				ArrayList<Integer> ports = index.getPortsWithFile(filename);
				int size = index.getFileSize(filename);
				int currentPort = index.getCurrentStoreFile(filename);

				if(currentPort >= ports.size()){
					sendClientMessage(Protocol.ERROR_LOAD_TOKEN);
					index.resetCurrentStoreFile(filename);
					return;
				}

				sendClientMessage(Protocol.LOAD_FROM_TOKEN + " " + ports.get(currentPort) + " " + size);
			} catch (Exception e) {
				System.err.println("Error in getting filename");
				return;
			}
		}

		private boolean checkForLoadLock(String filename){
			synchronized (clientLock){

				if(index.currentStateStoring(filename)){
					sendClientMessage(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
					return true;
				}

				if(index.currentStateRemoving(filename)){
					sendClientMessage(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
					return true;
				}
				return false;
			}
		}

		private void storeFile(String message){
			try{
				String filename = message.split(" ")[1];
				int filesize = Integer.parseInt(message.split(" ")[2]);

				if(index.containsFilename(filename)){
					sendClientMessage(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
					return;
				}

				if(checkForStoreLock(message)){
					return;
				};


				index.setCurrentState(indexStoreInProgress + filename);

				List<Integer> ports = index.getRDStores(replicationFactor);
				StringBuilder sb = new StringBuilder("");
				for (Integer port: ports) {
					index.addDStoreFile(port, filename);
					sb.append(" " + port);
				}

				sendClientMessage(Protocol.STORE_TO_TOKEN + sb);


				int neededAcks = ports.size();
				AtomicInteger currentAcks = new AtomicInteger();
				long startTime = System.currentTimeMillis();

				//get all dstores with the port and listen for store ack
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
											currentAcks.addAndGet(1);
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
				if(currentAcks.get() == neededAcks){
					sendClientMessage(Protocol.STORE_COMPLETE_TOKEN);
					index.addFileSizes(filename, filesize);
				} else{
					index.removeFiles(filename);
				}

				index.removeCurrentState(indexStoreInProgress + filename);
			} catch (Exception e){
				System.err.println("Error in getting ports to send to");
				e.printStackTrace();
			}
		}

		private boolean checkForStoreLock(String filename){
			synchronized (clientLock){
				if(index.currentStateStoring(filename)){
					sendClientMessage(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
					return true;
				}

				if(index.currentStateRemoving(filename)){
					sendClientMessage(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
					return true;
				}
				return false;
			}
		}

		private void deleteFile(String message){
			try{
				String filename = message.split(" ")[1];
				if(!index.containsFilename(filename)) {
					sendClientMessage(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
					return;
				}

				if(checkForRemoveLock(filename)){
					return;
				}

				index.removeFiles(filename);

				index.setCurrentState(indexRemoveInProgress + filename);
				ArrayList<Integer> ports = index.getPortsWithFile(filename);

				int neededAcks = ports.size();
				AtomicInteger currentAcks = new AtomicInteger();
				long startTime = System.currentTimeMillis();

				//get all dstores with the port and listen for delete ack
				for (DstoreMessageWriter dstore: dstores){
					if(ports.contains(dstore.getPort())){
						new Thread(() ->{
							dstore.sendDStoreMessage(Protocol.REMOVE_TOKEN + " " + filename);
							while(true){
								if(System.currentTimeMillis() - startTime >= timeout){
									System.err.println("Could not connect to DStore: " + dstore.getPort() + " in time");
									break;
								}
								String line;
								try{
									if((line = dstore.messageReader.readLine()) != null){
										if(line.equals(Protocol.REMOVE_ACK_TOKEN + " " + filename)){
											currentAcks.addAndGet(1);
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
				if(currentAcks.get() == neededAcks){
					sendClientMessage(Protocol.REMOVE_COMPLETE_TOKEN);
					index.setCurrentState(indexRemoveInProgress + filename);
				}
			} catch (Exception e){
				System.err.println("Error in getting ports to send to");
				e.printStackTrace();
			}
		}

		private boolean checkForRemoveLock(String filename){
			synchronized (clientLock){
				if(index.currentStateStoring(filename)){
					sendClientMessage(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
					return true;
				}

				if(index.currentStateRemoving(filename)){
					sendClientMessage(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
					return true;
				}
				return false;
			}
		}
	}

	public class DstoreMessageWriter {

		private Socket socket;
		private BufferedReader messageReader;
		private PrintWriter messageWriter;
		private int port = 0;

		public DstoreMessageWriter(Socket socket, BufferedReader messages, String line, PrintWriter messageWriter){
			this.socket = socket;
			this.messageReader = messages;
			this.messageWriter = messageWriter;
			try{
				this.port = Integer.parseInt(line.split(" ")[1]);
			} catch (Exception e){
				System.err.println(e.getMessage());
			}

			handleMessage(line);
		}


		public void handleMessage(String message){
			System.out.println("Message: \"" + message + "\" received from DStore");

			String splitMessage = message.split(" ")[0];
			switch(splitMessage){
				case Protocol.JOIN_TOKEN:
					sendDStoreMessage(Protocol.LIST_TOKEN);
					addFileToIndex();
					break;
			}
		}

		public void sendDStoreMessage(String message){
			System.out.println("Sending message \"" + message + "\" to DStore: "+ port);
			messageWriter.println(message);
			messageWriter.flush();
		}

		private void addFileToIndex(){
			try{
				String message;
				long startTime = System.currentTimeMillis();
				while(true){
					if(System.currentTimeMillis() - startTime >= timeout){
						//TODO: ERROR
						return;
					}
					if ((message = messageReader.readLine()) != null){
						break;
					}
				}

				String[] splitMessage = message.split(" ");
				ArrayList<String> listOfFiles = new ArrayList<>();

				if(splitMessage.length <= 1){
					System.out.println("DStore contains no files");
					index.addFiles(port, listOfFiles);
					return;
				}

				for(int i = 1; i < splitMessage.length; i++){
					listOfFiles.add(splitMessage[i]);
				}
				index.addFiles(port, listOfFiles);
			} catch(Exception e){
				System.err.println(e.getMessage());
				System.err.println("Error in splitting list message");
			}
		}

		public int getPort(){
			return port;
		}
	}
}
