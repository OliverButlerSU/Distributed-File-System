public interface MessageReceiverWriter extends Runnable {
	void handleMessage(String message);
}
