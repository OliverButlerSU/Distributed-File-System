package Worksheet;

import java.io.*;
import java.net.*;
class TCPReceiver {
	public static void main(String [] args) {
		ServerSocket ss = null;
		try {
			ss = new ServerSocket(4322);
			while(true) {
				try {
					Socket client = ss.accept();
					BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
					String line;
					while((line = in.readLine()) != null) System.out.println(line+" received");
					client.close();
				} catch(Exception e) { System.err.println("error: " + e); }
			}
		} catch(Exception e) { System.err.println("error: " + e);
		} finally {
			if (ss != null)
				try { ss.close(); } catch (IOException e) { System.err.println("error: " + e); }
		}
	}
}