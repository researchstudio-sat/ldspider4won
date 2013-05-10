package com.ontologycentral.ldspider.hooks.fetch;

import com.ontologycentral.ldspider.persist.CrawlStateManager;
import org.apache.http.HttpEntity;

import java.net.URI;

/**
 * FetchFilter implementation that uses the CrawlStateManager to determine if the URI should be accessed.
 */
public class AllowOnlyNewOrExpiredFetchFilter implements FetchFilter{
    private CrawlStateManager crawlStateManager = null;

    @Override
    public boolean fetchOk(URI u, int status, HttpEntity hen) {
        return this.crawlStateManager.isDownloadRequired(u);
    }

    public void setCrawlStateManager(CrawlStateManager crawlStateManager) {
        this.crawlStateManager = crawlStateManager;
    }
}
