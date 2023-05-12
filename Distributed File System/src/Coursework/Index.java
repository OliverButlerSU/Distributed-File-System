package Coursework;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Index {

	private ArrayList<String> currentState = new ArrayList<>();

	//port -> files[]
	//Contains all the DStores and what files they have
	private final HashMap<Integer, ArrayList<String>> dstoreFiles = new HashMap<>();

	//FileName -> FileSizes
	private final HashMap<String, Integer> dstoreFileSizes = new HashMap<>();

	//FileName -> Length of Dstore
	private HashMap<String, Integer> fileDStoresAccessed = new HashMap<>();

	private final String indexStoreInProgress = "STORE_IN_PROGRESS ";
	private final String indexStoreComplete = "STORE_COMPLETE ";
	private final String indexRemoveInProgress = "REMOVE_IN_PROGRESS ";
	private final String indexRemoveComplete = "REMOVE COMPLETE ";

	public Index(){

	}

	public void addFiles(int port, ArrayList<String> files){
		System.out.println("Added " + files.size() + " files from port: "+ port);
		dstoreFiles.put(port, files);
	}

	public void addFileSizes(String filename, int size){
		dstoreFileSizes.put(filename, size);
		fileDStoresAccessed.put(filename, 0);
	}

	public void addDStoreFile(int port, String filename){
		dstoreFiles.get(port).add(filename);
	}

	public void removeFiles(String filename){
		dstoreFiles.values().stream().forEach((val) -> val.remove(filename));
		dstoreFileSizes.remove(filename);
		fileDStoresAccessed.remove(filename);
	}

	public void removeDStore(int dstorePort){
		dstoreFileSizes.remove(dstorePort);
	}

	public String getAllFiles(){
		Set<String> ports = dstoreFileSizes.keySet();
		return String.join(" ", ports);
	}

	public ArrayList<Integer> getPortsWithFile(String filename) {
		ArrayList<Integer> dstores = new ArrayList<>();

		for(Integer key: dstoreFiles.keySet()){
			if(dstoreFiles.get(key).contains(filename)){
				dstores.add(key);
			}
		}
		return dstores;
	}

	public List<Integer> getRDStores(int R){
		if(dstoreFiles.size() < R){
			//TODO: Return error
			return new ArrayList<>();
		}

		return sortRDStoresByLength().stream().limit(R).collect(Collectors.toList());
		// return dstoreFiles.keySet().stream().limit(R).collect(Collectors.toList());
	}

	public ArrayList<Integer> sortRDStoresByLength() {
		Integer[][] keyLengths = new Integer[dstoreFiles.size()][2];

		int i = 0;
		for (int key : dstoreFiles.keySet()) {
			keyLengths[i] = new Integer[]{key, dstoreFiles.get(key).size()};
			i++;
		}

		Arrays.sort(keyLengths, Comparator.comparingInt(a -> a[1]));

		ArrayList<Integer> finalPorts = new ArrayList<>();
		for (int j = 0; j < keyLengths.length; j++) {
			finalPorts.add(keyLengths[j][0]);
		}
		return finalPorts;
	}

	public boolean currentStateStoring(String filename){
		return currentState.contains(indexStoreInProgress + filename);
	}

	public boolean currentStateRemoving(String filename){
		return currentState.contains(indexRemoveInProgress + filename);
	}

	public void removeCurrentState(String state){
		currentState.remove(state);
	}

	public boolean containsFilename(String filename){
		return dstoreFileSizes.keySet().contains(filename);
	}

	public int getCurrentStoreFile(String filename){
		return fileDStoresAccessed.get(filename);
	}

	public void incrementCurrentStoreFiles(String filename){
		fileDStoresAccessed.put(filename, fileDStoresAccessed.get(filename) + 1);
	}

	public void resetCurrentStoreFile(String filename){
		fileDStoresAccessed.put(filename, 0);
	}

	public int getFileSize(String filename){
		return dstoreFileSizes.get(filename);
	}

	public void setCurrentState(String currentState) {
		this.currentState.add(currentState);
	}
}
