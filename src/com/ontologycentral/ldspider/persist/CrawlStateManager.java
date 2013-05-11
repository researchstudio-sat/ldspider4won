package com.ontologycentral.ldspider.persist;

import java.net.URI;
import java.util.Date;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: fkleedorfer
 * Date: 09.05.13
 * Time: 22:50
 * To change this template use File | Settings | File Templates.
 */
public interface CrawlStateManager {

    /**
     * Adds the resource to the persisted crawl state with the specified expiry date.
     * @param resourceUri
     * @param expires the expiry date or null if the resource does not expire.
     */
    public void registerExpiryDate(URI resourceUri, Date expires);

    /**
     * Adds the resource to the persisted crawl state, stating that it will never expire.
     * @param resourceUri
     */
    public void registerUriNeverExpires(URI resourceUri);

    /**
     * Checks the crawl persisted state to determine if the resource should be downloaded.
     * If the resource is unknown or the expiry date is in the past, true is returned.
     * @param resourceUri
     */
    public boolean isDownloadRequired(URI resourceUri);

    /**
     * Returns an iterator over all known expired URIs.
     * @return
     */
    public Iterator<URI> getExpiredUriIterator();

    /**
     * Releases all resources.
     */
    public void shutdown();

    /**
     * Initializes the crawl state manager.
     */
    void initialize();
}
