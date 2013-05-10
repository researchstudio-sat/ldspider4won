package com.ontologycentral.ldspider.persist;

import java.io.*;
import java.net.URI;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

/**
 * Vanilla implementation of the CrawlStateManager interface using a TreeMap that is serialized to file in a
 * folder that is specified upon instantiation.
 */
public class CrawlStateManagerImpl implements CrawlStateManager {
    public static final String EXPIRY_DATE_FILE = "expirydates";

    /**
     * Structure holding the expiry date for each URI.
     * URIs not present in the data structure will be re-accessed by the crawler
     * URIs with an expiry date in the past will be re-accessed by the crawler
     */
    private TreeMap<URI, Date> uriExpiryDateMap;

    private File dataFolder = null;

    private boolean initialized = false;

    private Date neverExpiresDate = null;


    /**
     * Creates a new instance which uses the specified folder for storing all state data.
     * The folder will be created if it doesn't exist.
     * @param dataFolder
     */
    public CrawlStateManagerImpl(File dataFolder) {
        this.dataFolder = dataFolder;
        //set a date 1000 years in the future, use that as 'never expires' date.
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.set(Calendar.YEAR, cal.get(Calendar.YEAR) + 1000);
    }

    @Override
    public void registerExpiryDate(URI resourceUri, Date expires) {
        synchronized (this){
            dieIfNotInitialized();
            this.uriExpiryDateMap.put(resourceUri, expires);
        }
    }

    @Override
    public void registerUriNeverExpires(URI resourceUri) {
        synchronized (this) {
            dieIfNotInitialized();
            this.uriExpiryDateMap.put(resourceUri, neverExpiresDate);
        }
    }

    @Override
    public boolean isDownloadRequired(URI resourceUri) {
        dieIfNotInitialized();
        Date expiryDate = this.uriExpiryDateMap.get(resourceUri);
        if (expiryDate == null) return true;
        return expiryDate.before(new Date());    //let's assume creating a new date each time does not eat too many resources
    }

    @Override
    public void shutdown() {
        try {
            synchronized (this) {
                dieIfNotInitialized();
                saveStateToDirectory(this.dataFolder);
                this.initialized = false;
            }
        } catch (Exception e) {
            throw new RuntimeException("Error occurred during shutdown of the crawl state manager", e);
        }
    }

    @Override
    public void initialize(){
        try {
            synchronized (this) {
                if (this.initialized) throw new IllegalStateException("Cannot initialize: already initialized.");
                initializeStateDirectory(this.dataFolder);
                this.initialized = true;
            }
        } catch (Exception e) {
            throw new RuntimeException("Error occurred during initialization of the crawl state manager", e);
        }
    }



    private void saveStateToDirectory(File folder) throws IOException {
        checkFolderPermissions(folder);
        File expiryDateFile = new File(folder, EXPIRY_DATE_FILE);
        writeMapToFile(expiryDateFile, this.uriExpiryDateMap);
    }

    private void initializeStateDirectory(File folder) {
        try {
            createFolderIfNotExists(folder);
            checkFolderPermissions(folder);
            File expiryDateFile = new File(folder, EXPIRY_DATE_FILE);
            if (expiryDateFile.exists()) {
                if (!expiryDateFile.canRead()) throw new IllegalArgumentException("Cannot initialize state from file '" + expiryDateFile + "': read access denied");
                //file exists - deserialize map
                this.uriExpiryDateMap = readMapFromFile(expiryDateFile);
            } else {
                //file doesn't exist, create new map
                this.uriExpiryDateMap = new TreeMap<URI, Date>();
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not initialize state with directory: '" + folder + "'", e);
        }
    }

    private TreeMap<URI, Date> readMapFromFile(File expiryDateFile) throws IOException, ClassNotFoundException {
        ObjectInputStream ois = null;
        FileInputStream fis = null;
        TreeMap<URI,Date> theMap = null;
        try {
            fis = new FileInputStream(expiryDateFile);        
            ois  = new ObjectInputStream(fis);
            theMap = (TreeMap<URI, Date>) ois.readObject();
        } finally {
            try {
                if (fis != null) fis.close();
            } catch ( Exception e) { 
                throw new RuntimeException("couldn't close file input stream on '" + expiryDateFile +"'",e);
            }
            try {
                if (ois != null) ois.close();
            } catch ( Exception e) {
                throw new RuntimeException("couldn't close object input stream on '" + expiryDateFile +"'",e);
            }
        }
        return theMap;
    }
    
    private void writeMapToFile(File expiryDateFile, Map expiryDateMap) throws IOException {
        ObjectOutputStream oos = null;
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(expiryDateFile);
            oos  = new ObjectOutputStream(fos);
            oos.writeObject(expiryDateMap);
        } finally {
            try {
                if (fos != null) fos.close();
            } catch ( Exception e) {
                throw new RuntimeException("couldn't close file output stream on '" + expiryDateFile +"'",e);
            }
            try {
                if (oos != null) oos.close();
            } catch ( Exception e) {
                throw new RuntimeException("couldn't close object output stream on '" + expiryDateFile +"'",e);
            }
        }        
    }

    private void dieIfNotInitialized(){
        if (!initialized) {
            throw new IllegalStateException("Cannot use CrawlStateManager: not initialized. Maybe the initialization failed?");
        }
    }

    private void createFolderIfNotExists(File folder) {
        if (!folder.exists()) {
            folder.mkdirs();
        }
    }
    
    private void checkFolderPermissions(File folder) {
        if (!folder.isDirectory()) throw new IllegalArgumentException("Not a folder: '" + folder.getName() +"'");
        if (!folder.canWrite()) throw new IllegalArgumentException("Cannot write to folder '" + folder.getName() +"'");        
    }

    private void checkFilePermissions(File file) {
        if (!file.isFile()) throw new IllegalArgumentException("Not a file: '" + file.getName() +"'");
        if (!file.canWrite()) throw new IllegalArgumentException("Cannot write to file '" + file.getName() +"'");
    }
}
