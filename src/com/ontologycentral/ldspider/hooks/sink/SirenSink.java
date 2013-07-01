/*
 * Copyright 2012  Research Studios Austria Forschungsges.m.b.H.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.ontologycentral.ldspider.hooks.sink;

import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.Callback;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SirenSink implements Sink
{

  private final Logger _log = Logger.getLogger(this.getClass().getSimpleName());

  private static final String DEFAULT_SOLR_SERVER_URL = "http://sat001:8080/siren";
  private CommonsHttpSolrServer solrServer = null;

  private static final String FIELD_URL = "url";
  private static final String FIELD_NTRIPLE = "ntriple";

  private static final String FIELD_TITLE = "title";
  private static final String FIELD_DESCRIPTION = "description";
  private static final String FIELD_BASIC_NEED_TYPE = "basicNeedType";
  private static final String FIELD_LOCATION = "location";


  //hack: thanks to httprange-14, we have to determine the resource url by
  // searching for triples X won:hasConnections Y. So we have to define this constant here
  //TODO: the URI here may change in the future, which may break the siren/sink code
  private static final String WON_PREFIX = "http://purl.org/webofneeds/model#";
  private static final String DC_PREFIX = "http://purl.org/dc/elements/1.1/";
  private static final String RDF_PREFIX = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
  public static final String GEO_PREFIX = "http://www.w3.org/2003/01/geo/wgs84_pos#";

  private static final String RDF_TYPE = RDF_PREFIX + "type";

  private static final String WON_NEED = WON_PREFIX + "Need";
  private static final String WON_HAS_CONNECTIONS = WON_PREFIX + "hasConnections";
  private static final String WON_TEXT_DESCRIPTION = WON_PREFIX + "hasTextDescription";
  private static final String WON_BASIC_NEED_TYPE = WON_PREFIX + "hasBasicNeedType";
  private static final String WON_BELONGS_TO_NEED = "http://purl.org/webofneeds/model#belongsToNeed";

  private static final String DC_TITLE = DC_PREFIX + "title";

  private static final String GEO_LONGITUDE = GEO_PREFIX + "longitude";
  private static final String GEO_LATITUDE = GEO_PREFIX + "latitude";

  public SirenSink(String serverURI)
  {
    _log.info("connecting to solr server at " + serverURI);
    try {
      this.solrServer = new CommonsHttpSolrServer(serverURI != null ? serverURI : DEFAULT_SOLR_SERVER_URL);
    } catch (MalformedURLException e) {
      _log.log(Level.WARNING, "could not create http solr client", e);
    }
  }

  @Override
  public void shutdown()
  {
    if (this.solrServer != null) {
      try {
        _log.info("shutting down siren sink, committing siren/solr data");
        this.solrServer.commit();
      } catch (Exception e) {
        _log.log(Level.WARNING, "error committing siren/solr data", e);
      }
    }
  }

  public Callback newDataset(Provenance provenance)
  {
    return new SirenCallback(provenance, solrServer);
  }

  public void close()
  {
    if (this.solrServer == null) return;
    _log.info("shutting down siren solr connection..");
    _log.info("siren sink closed.");
  }

  /**
   * Do nothing
   *
   * @author aharth
   */
  private static class SirenCallback implements Callback
  {
    private final Logger _log = Logger.getLogger(this.getClass().getSimpleName());

    private Provenance provenance;
    private SolrInputDocument document = null;
    private CommonsHttpSolrServer solrServer;

    private StringBuilder nTriplesBuilder = null;
    private String documentUrl = null;

    private boolean isNeed = false;

    private String title = null;
    private String description = null;
    private String basicNeedType = null;
    private String latitude = null;
    private String longitude = null;

    private SirenCallback(Provenance provenance, CommonsHttpSolrServer server)
    {
      this.provenance = provenance;
      this.solrServer = server;
    }

    @Override
    public void startDocument()
    {
      _log.info("starting to collect statements of document for siren/solr server, doc url is " + this.provenance.getUri().toString());
      this.document = new SolrInputDocument();
      this.document.addField(FIELD_URL, "");
      this.document.addField(FIELD_NTRIPLE, "");

      this.document.addField(FIELD_TITLE, "");
      this.document.addField(FIELD_DESCRIPTION, "");
      this.document.addField(FIELD_BASIC_NEED_TYPE, "");

      this.nTriplesBuilder = new StringBuilder();
      this.documentUrl = null;
    }

    @Override
    public void processStatement(Node[] nx)
    {
      //_log.info("collecting statements for siren/solr, doc url is " + this.provenance.getUri().toString() + ", data: " + Arrays.toString(nx));
      try {
        writeStatement(nx);
      } catch (Exception e) {
        _log.log(Level.WARNING, "error processing triple '" + Arrays.toString(nx) + "'.", e);
      }
    }

    /**
     * Adds a statement to the current SPARQL/Update request.
     * (stolen from SinkSparul.java)
     *
     * @param nodes The statement
     * @throws java.io.IOException
     */
    private void writeStatement(Node[] nodes)
    {
      //Preconditions
      if (this.nTriplesBuilder == null)
        startDocument(); //there seems to be some kind of race condition that causes writeStatement to be called before startDocument sometimes

      if (nodes == null) throw new NullPointerException("nodes must not be null");

      if (nodes.length < 3) throw new IllegalArgumentException("A statement must consist of at least 3 nodes");

      //hack:
      if (this.documentUrl == null && WON_HAS_CONNECTIONS.equals(nodes[1].toString())) {
        this.documentUrl = nodes[0].toString();
        _log.info("using this as document url: " + this.documentUrl);
      }

      if (nodes[1].toString().equals(RDF_TYPE) && nodes[2].toString().equals(WON_NEED))
        isNeed = true;

      if (title == null && nodes[1].toString().equals(DC_TITLE))
        title = nodes[2].toString();

      if (description == null && nodes[1].toString().equals(WON_TEXT_DESCRIPTION))
        description = nodes[2].toString();

      if (basicNeedType == null && nodes[1].toString().equals(WON_BASIC_NEED_TYPE))
        basicNeedType = nodes[2].toString();

      if (latitude == null && nodes[1].toString().equals(GEO_LATITUDE))
        latitude = nodes[2].toString();

      if (longitude == null && nodes[1].toString().equals(GEO_LONGITUDE))
        longitude = nodes[2].toString();

      nTriplesBuilder.append(nodes[0].toN3() + " " + nodes[1].toN3() + " " + nodes[2].toN3() + " .\n");
    }

    @Override
    public void endDocument()
    {
      if (!isNeed) {
        _log.info("Not a Need graph, skipping.");
        return;
      }

      if (this.documentUrl == null) {
        _log.warning("Could not determine document url by analyzing triples. Using " + this.provenance.getUri() + " as a fallback. This may cause problems.");
        this.documentUrl = this.provenance.getUri().toString();
      }
      this.document.setField(FIELD_URL, this.documentUrl);

      if (title != null)
        document.setField(FIELD_TITLE, title);

      if (description != null)
        document.setField(FIELD_DESCRIPTION, description);

      if (basicNeedType != null)
        document.setField(FIELD_BASIC_NEED_TYPE, basicNeedType);

      if (latitude != null && longitude != null)
        document.addField(FIELD_LOCATION, latitude + "," + longitude);

      writeToSiren();
    }

    private void writeToSiren()
    {
      _log.info("writing document to siren/solr server, doc url is " + this.provenance.getUri().toString() + ", using resource url " + this.documentUrl);
      try {
        String nTriplesString = nTriplesBuilder.toString();

        //reject anything without the triples
        if (nTriplesString.length() == 0) return;
        _log.fine("writing these triples: \n" + nTriplesString);
        this.document.setField(FIELD_NTRIPLE, nTriplesString);

        final UpdateRequest request = new UpdateRequest();
        request.add(this.document);
        request.process(this.solrServer);

        //TODO: find out how to fix this
        //Thread.sleep(1500); //the timing code in LoadBalancingQueue makes this necessary for hosts without tld in the LAN.
        //with breadth-first ("-b") it seems to work, though.

        /*
        SolrQuery params = new SolrQuery();
        params.set("mlt", "true");
        params.set(MoreLikeThisParams.MIN_DOC_FREQ,1);
        params.set(MoreLikeThisParams.MIN_TERM_FREQ,1);
        params.set(MoreLikeThisParams.SIMILARITY_FIELDS,FIELD_NTRIPLE);
        params.setQueryType("siren");
        params.set("q", "id:" + this.provenance.getUri().toString());

        QueryResponse response = this.solrServer.query(params);
        SolrDocumentList results = response.getResults();
        _log.info("results for mlt query: " + results.size() + " documents");
        for( SolrDocument result : results) {
            _log.info("got result: " + result.getFirstValue("id"));
        }*/


      } catch (Exception e) {
        _log.log(Level.WARNING, "An error occurred when writing to solr server at " + solrServer.getBaseURL(), e);
      }
    }
  }

}
