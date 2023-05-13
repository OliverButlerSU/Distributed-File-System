import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Index {

    /** Contains the states of all files being stored and removed */
    private final ArrayList<String> currentState = new ArrayList<>();

	/**
	 * Contains all the DStores with what files they store
	 * port -> filenames
	 */
	private final HashMap<Integer, ArrayList<String>> dstoreFiles = new HashMap<>();

	/**
	 * Stores the file sizes for each file
	 * filename -> filesize
	 */
	private final HashMap<String, Integer> dstoreFileSizes = new HashMap<>();

	/**
	 * Stores the amount of dstores accessed to retreieve a file after reloading
	 * filename -> accessed count
	 */
	private final HashMap<String, Integer> fileDStoresAccessed = new HashMap<>();

	/** Index file state for storing in progress */
	private final String indexStoreInProgress = "STORE_IN_PROGRESS ";

	/** Index file state for removing in progress */
	private final String indexRemoveInProgress = "REMOVE_IN_PROGRESS ";

    /**
     * Adds a list of files to a DStore
     *
     * @param port DStore port
     * @param files file names
     */
    public void addFiles(int port, ArrayList<String> files) {
        System.out.println("Added " + files.size() + " files from port: " + port);
        dstoreFiles.put(port, files);
    }

    /**
     * Adds the file sizes to each file
     *
     * @param filename name of file
     * @param size size of file
     */
    public void addFileSizes(String filename, int size) {
        dstoreFileSizes.put(filename, size);
        fileDStoresAccessed.put(filename, 0);
    }

    /**
     * Adds a file to a DStore
     *
     * @param port DStore port
     * @param filename file name
     */
    public void addDStoreFile(int port, String filename) {
        dstoreFiles.get(port).add(filename);
    }

    /**
     * Removes a file from all hashmaps
     *
     * @param filename name of file
     */
    public void removeFiles(String filename) {
        dstoreFiles.values().forEach((val) -> val.remove(filename));
        dstoreFileSizes.remove(filename);
        fileDStoresAccessed.remove(filename);
    }

    /**
     * Gets all files for the LIST token
     *
     * @return list of all files
     */
    public String getAllFiles() {
        Set<String> ports = dstoreFileSizes.keySet();
        return String.join(" ", ports);
    }

    /**
     * Get all DStores with a file
     *
     * @param filename name of file
     * @return List of all DStores
     */
    public ArrayList<Integer> getPortsWithFile(String filename) {
        ArrayList<Integer> dstores = new ArrayList<>();

        for (Integer key : dstoreFiles.keySet()) {
            if (dstoreFiles.get(key).contains(filename)) {
                dstores.add(key);
            }
        }
        return dstores;
    }

    /**
     * Get R Dstores to store files onto
     *
     * @param R Replication Factor
     * @return List of DStores
     */
    public List<Integer> getRDStores(int R) {
        if (dstoreFiles.size() < R) {
            // TODO: Return error
            return new ArrayList<>();
        }

        return sortRDStoresByLength().stream().limit(R).collect(Collectors.toList());
    }

    /**
     * Sort DStores by the amount of files it is currently storing
     *
     * @return List of DStores
     */
    public ArrayList<Integer> sortRDStoresByLength() {
        Integer[][] keyLengths = new Integer[dstoreFiles.size()][2];

        // [DStorePort][Amount of Files Stored]
        int i = 0;
        for (int key : dstoreFiles.keySet()) {
            keyLengths[i] = new Integer[] {key, dstoreFiles.get(key).size()};
            i++;
        }

        // Sort by files stored
        Arrays.sort(keyLengths, Comparator.comparingInt(a -> a[1]));

        // Return a list of DStore ports
        ArrayList<Integer> finalPorts = new ArrayList<>();
        for (Integer[] keyLength : keyLengths) {
            finalPorts.add(keyLength[0]);
        }
        return finalPorts;
    }

    /**
     * Checks if a file is currently being stored
     *
     * @param filename file name
     * @return boolean
     */
    public boolean currentStateStoring(String filename) {
        return currentState.contains(indexStoreInProgress + filename);
    }

    /**
     * Checks if a file is currently being removed
     *
     * @param filename file name
     * @return boolean
     */
    public boolean currentStateRemoving(String filename) {
        return currentState.contains(indexRemoveInProgress + filename);
    }

    /**
     * Remove the current state of a file
     *
     * @param state state of file
     */
    public void removeCurrentState(String state) {
        currentState.remove(state);
    }

    /**
     * Checks if a file is being stored by a DStore
     *
     * @param filename file name
     * @return bool
     */
    public boolean containsFilename(String filename) {
        return dstoreFileSizes.containsKey(filename);
    }

    /**
     * Get the amount of times a file has been accessed using reload
     *
     * @param filename name of file
     * @return amount of times a file has been accessed using reload
     */
    public int getCurrentStoreFile(String filename) {
        return fileDStoresAccessed.get(filename);
    }

    /**
     * Increments the amount of times a file has been accessed after being reloaded
     *
     * @param filename name of file
     */
    public void incrementCurrentStoreFiles(String filename) {
        fileDStoresAccessed.put(filename, fileDStoresAccessed.get(filename) + 1);
    }

    /**
     * Resets the amount of times a file has been accessed using reload
     *
     * @param filename name of tile
     */
    public void resetCurrentStoreFile(String filename) {
        fileDStoresAccessed.put(filename, 0);
    }

    /**
     * Gets the file size of a file
     *
     * @param filename name of file
     * @return size of file in bytes
     */
    public int getFileSize(String filename) {
        return dstoreFileSizes.get(filename);
    }

    /**
     * Set the state of a file
     *
     * @param currentState state of file
     */
    public void setCurrentState(String currentState) {
        this.currentState.add(currentState);
    }
}
