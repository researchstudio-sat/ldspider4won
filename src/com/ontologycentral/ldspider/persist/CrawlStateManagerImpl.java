package com.ontologycentral.ldspider.persist;

import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

/**
 * Vanilla implementation of the CrawlStateManager interface using a TreeMap that is serialized to file in a
 * folder that is specified upon instantiation.
 */
public class CrawlStateManagerImpl implements CrawlStateManager {
    public static final String EXPIRY_DATE_FILE = "expirydates";


  Logger _log = Logger.getLogger(this.getClass().getSimpleName());


    /**
     * Structure holding the expiry date for each URI.
     * URIs not present in the data structure will be re-accessed by the crawler
     * URIs with an expiry date in the past will be re-accessed by the crawler
     */
    private TreeMap<URI, Date> uriExpiryDateMap;

    private File dataFolder = null;

    private boolean initialized = false;

    private Date neverExpiresDate = null;

    private static final String LOG_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";


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
        this.neverExpiresDate = cal.getTime();
    }

    public void logCrawlState(){
        Date currentDate = new Date();
        _log.info("-- START logging crawl state --");
        _log.info("            using data folder: " + dataFolder);
        _log.info("date used for 'never expires': " + neverExpiresDate);
        _log.info("                  initialized: " + initialized);
        _log.info("                 current date: " + currentDate);
        _log.info(" URI expiry dates:");
        Iterator<Map.Entry<URI, Date>> expiryDates = this.uriExpiryDateMap.entrySet().iterator();
        while (expiryDates.hasNext()){
            Map.Entry<URI,Date> expiryDate = expiryDates.next();
            String expiredStr = expiryDate.getValue().before(currentDate)? "expired":"not expired";
            _log.info(expiredStr + ": " + expiryDate.getKey() + " (" + expiryDate.getValue() + ")");
        }
        _log.info("--  END  logging crawl state --");
    }


    @Override
    public void registerExpiryDate(URI resourceUri, Date expiryDate) {
        synchronized (this){
            dieIfNotInitialized();
            _log.info("registering expiry date '" + (expiryDate==null? "[null]":new SimpleDateFormat(LOG_DATE_FORMAT).format(expiryDate)) + "' for URI '" + resourceUri +"'" );
            this.uriExpiryDateMap.put(resourceUri, expiryDate);
        }
    }

    @Override
    public void registerUriNeverExpires(URI resourceUri) {
        synchronized (this) {
            dieIfNotInitialized();
            _log.info("registering expiry date '" + new SimpleDateFormat(LOG_DATE_FORMAT).format(neverExpiresDate) +"' (i.e. never expires) for URI '" + resourceUri +"'" );
            this.uriExpiryDateMap.put(resourceUri, neverExpiresDate);
        }
    }

    @Override
    public boolean isDownloadRequired(URI resourceUri) {
        dieIfNotInitialized();
        Date expiryDate = this.uriExpiryDateMap.get(resourceUri);
        _log.info("checking expiry date '" + (expiryDate==null? "[null]":new SimpleDateFormat(LOG_DATE_FORMAT).format(expiryDate)) + "' for URI '" + resourceUri +"'" );
        if (expiryDate == null) return true;  // we haven't seen this URI yet -> download.
        return expiryDate.before(new Date());    //URI is expired? If yes, download. (let's assume creating a new date each time does not eat too many resources)
    }

    @Override
    public Iterator<URI> getExpiredUriIterator()
    {
      dieIfNotInitialized();
      final Date now = new Date();
      final Iterator<Map.Entry<URI,Date>> baseIterator = uriExpiryDateMap.entrySet().iterator();
      return new Iterator<URI>()
      {
        private URI next = getNextExpiredURI();

        private URI getNextExpiredURI()
        {
          while(baseIterator.hasNext()){
            Map.Entry<URI,Date> candidate = baseIterator.next();
            if (candidate.getValue().before(now)) return candidate.getKey();
          }
          return null;
        }

        @Override
        public boolean hasNext()
        {
          return next != null;
        }

        @Override
        public URI next()
        {
          URI toReturn = next;
          next = getNextExpiredURI();
          return toReturn;
        }

        @Override
        public void remove()
        {
          throw new UnsupportedOperationException("not implemented");
        }
      };
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
                _log.info("initializing CrawlStateManagerImpl");
                if (this.initialized) throw new IllegalStateException("Cannot initialize: already initialized.");
                initializeStateDirectory(this.dataFolder);
                this.initialized = true;
                _log.info("done initializing CrawlStateManagerImpl");
            }
        } catch (Exception e) {
            throw new RuntimeException("Error occurred during initialization of the crawl state manager", e);
        }
    }



    private void saveStateToDirectory(File folder) throws IOException {
        _log.info("saving crawl state to folder '" + folder.getAbsolutePath() +"'");
        checkFolderPermissions(folder);
        File expiryDateFile = new File(folder, EXPIRY_DATE_FILE);
        writeMapToFile(expiryDateFile, this.uriExpiryDateMap);
        _log.info("finished saving crawl state");
    }

    private void initializeStateDirectory(File folder) {
        _log.info("initializing crawl state folder: '" + folder.getAbsolutePath() +"'");
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
        _log.info("finished initializing crawl state folder");
    }

    private TreeMap<URI, Date> readMapFromFile(File expiryDateFile) throws IOException, ClassNotFoundException {
        _log.info("reading expiry dates from file '" + expiryDateFile.getAbsolutePath()+"'");
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
        _log.info("finished reading expiry dates");
        return theMap;
    }
    
    private void writeMapToFile(File expiryDateFile, Map expiryDateMap) throws IOException {
        _log.info("writing expiry dates to file '" + expiryDateFile.getAbsolutePath()+"'");
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
        _log.info("done writing expiry dates");
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
