package Worksheet;

import java.io.*;
import java.net.*;
class TCPReceiverThreadedClass {
	public static void main(String [] args) {
		ServerSocket ss = null;
		try {
			ss = new ServerSocket(4322);
			int num = 0;
			while (true) {
				try {
					Socket client = ss.accept();
					new Thread(new ServiceThread(client, num++)).start();
				} catch(Exception e) { System.err.println("error: " + e); }
			}
		} catch(Exception e) { System.err.println("error: " + e);
		} finally {
			if (ss != null)
				try { ss.close(); } catch (IOException e) { System.err.println("error: " + e); }
		}
	}
	static class ServiceThread implements Runnable {
		Socket client;
		int num;
		ServiceThread(Socket c, int num) {
			client=c;
			this.num = num;
		}
		public void run() {
			try {
				BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
				String line;
				while((line = in.readLine()) != null)
					System.out.println(line+" received by "+ num);
				client.close();
			} catch(Exception e) {
				System.err.println("error: " + e);
			}
		}
	}
}
