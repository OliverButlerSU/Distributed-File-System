package Coursework;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Index {

	private ArrayList<String> currentState = new ArrayList<>();

	//port -> files[]
	//Contains all the DStores and what files they have
	private final HashMap<Integer, ArrayList<String>> dstoreFiles = new HashMap<>();

	//Files -> FileSizes
	private final HashMap<String, Integer> dstoreFileSizes = new HashMap<>();

	//FileName -> Amount of dstores currently accessed to get to it
	private HashMap<String, Integer> fileDStoresAccessed = new HashMap<>();

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

		return dstoreFiles.keySet().stream().limit(R).collect(Collectors.toList());
	}

	public String currentFileState(String filename){
		String state = null;

		for(int i = 0; i < currentState.size(); i++){
			String st = currentState.get(i);

			if(st.split(" ")[1].equals(filename)){
				state = st.split(" ")[0];
				break;
			}
		}
		return state;
	}

	public void removeCurrentState(String filename){
		for(int i = 0; i < currentState.size(); i++){
			String st = currentState.get(i);

			if(st.split(" ")[1].equals(filename)){
				currentState.remove(i);
				break;
			}
		}
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
