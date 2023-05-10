package Worksheet;

import java.io.*;
import java.net.*;
class TCPSender {
	public static void main(String [] args) {
		Socket socket = null;
		try {
			InetAddress address = InetAddress.getLocalHost();
			socket = new Socket(address, 4322);
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			for(int i = 0; i < 10; i++) {
				out.println("TCP message "+i);
				Thread.sleep(1000);
			}
		} catch(Exception e) { System.err.println("error: " + e);
		} finally {
			if (socket != null)
				try { socket.close(); } catch (IOException e) { System.err.println("error: " + e); }
		}
	}
}
