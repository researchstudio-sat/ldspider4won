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
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SirenSink implements Sink
{

  private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

  private static final String DEFAULT_SOLR_SERVER_URL = "http://sat001:8080/siren";
  private CommonsHttpSolrServer solrServer = null;

  //FIELDS! they have to be the same as in the won-core/SolrFields class
  private static final String FIELD_URL = "url";
  private static final String FIELD_NTRIPLE = "ntriple";

  private static final String FIELD_TITLE = "title";
  private static final String FIELD_DESCRIPTION = "description";
  private static final String FIELD_BASIC_NEED_TYPE = "basicNeedType";
  private static final String FIELD_LOCATION = "location";
  private static final String FIELD_LOWERPRICE = "lowerPriceLimit";
  private static final String FIELD_UPPERPRICE = "upperPriceLimit";
  private static final String FIELD_STARTTIME = "startTime";
  private static final String FIELD_ENDTIME = "endTime";

  private static final String FIELD_TAG = "tag";

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
  private static final String WON_HAS_TAG = WON_PREFIX + "hasTag";
  private static final String WON_HAS_LOWER_PRICE_LIMIT = WON_PREFIX + "hasLowerPriceLimit";
  private static final String WON_HAS_UPPER_PRICE_LIMIT = WON_PREFIX + "hasUpperPriceLimit";
  private static final String WON_HAS_START_TIME = WON_PREFIX + "hasStartTime";
  private static final String WON_HAS_END_TIME = WON_PREFIX + "hasEndTime";

  private static final String DC_TITLE = DC_PREFIX + "title";

  private static final String GEO_LONGITUDE = GEO_PREFIX + "longitude";
  private static final String GEO_LATITUDE = GEO_PREFIX + "latitude";

  public SirenSink(String serverURI)
  {
    logger.info("connecting to solr server at " + serverURI);
    try {
      this.solrServer = new CommonsHttpSolrServer(serverURI != null ? serverURI : DEFAULT_SOLR_SERVER_URL);
    } catch (MalformedURLException e) {
      logger.log(Level.WARNING, "could not create http solr client", e);
    }
  }

  @Override
  public void shutdown()
  {
    if (this.solrServer != null) {
      try {
        logger.info("shutting down siren sink, committing siren/solr data");
        this.solrServer.commit();
      } catch (Exception e) {
        logger.log(Level.WARNING, "error committing siren/solr data", e);
      }
    }
  }

  @Override
  public Callback newDataset(Provenance provenance)
  {
    return new SirenCallback(provenance, solrServer);
  }

  public void close()
  {
    if (this.solrServer == null) return;
    logger.info("shutting down siren solr connection..");
    logger.info("siren sink closed.");
  }

  /**
   * Do nothing
   *
   * @author Florian Kleedorfer & Alan Tus
   */
  private static class SirenCallback implements Callback
  {
    private final Logger logger = Logger.getLogger(this.getClass().getName());

    private Provenance provenance;
    private CommonsHttpSolrServer solrServer;

    private SolrInputDocument document = null;
    private String documentUrl = null;

    private StringBuilder nTriplesBuilder = null;

    private final String TAG_PATTERN_STRING = "#\\w+";
    private Pattern tagPattern;

    private boolean isNeed = false;

    private String title = null;
    private String description = null;
    private String basicNeedType = null;
    private float latitude = 0;
    private float longitude = 0;
    private double priceLower = -1;
    private double priceUpper = -1;
    private Date startTime = null;
    private Date endTime = null;

    private SirenCallback(Provenance provenance, CommonsHttpSolrServer server)
    {
      this.provenance = provenance;
      this.solrServer = server;
      this.tagPattern = Pattern.compile(TAG_PATTERN_STRING);
    }

    @Override
    public void startDocument()
    {
      logger.info("starting to collect statements of document for siren/solr server, doc url is " + this.provenance.getUri().toString());

      this.document = new SolrInputDocument();
      this.nTriplesBuilder = new StringBuilder();
    }

    @Override
    public void processStatement(Node[] nx)
    {
      //logger.info("collecting statements for siren/solr, doc url is " + this.provenance.getUri().toString() + ", data: " + Arrays.toString(nx));
      try {
        writeStatement(nx);
      } catch (Exception e) {
        logger.log(Level.WARNING, "error processing triple '" + Arrays.toString(nx) + "'.", e);
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
        logger.info("using this as document url: " + this.documentUrl);
      }

      if (nodes[1].toString().equals(RDF_TYPE) && nodes[2].toString().equals(WON_NEED))
        isNeed = true;

      if (title == null && nodes[1].toString().equals(DC_TITLE)) {
        title = nodes[2].toString();
        parseTags(title);
      }

      if (description == null && nodes[1].toString().equals(WON_TEXT_DESCRIPTION)) {
        description = nodes[2].toString();
        parseTags(description);
      }

      if (basicNeedType == null && nodes[1].toString().equals(WON_BASIC_NEED_TYPE))
        basicNeedType = nodes[2].toString();

      try {
        if (nodes[1].toString().equals(GEO_LATITUDE))
          latitude = Float.parseFloat(nodes[2].toString());
        if (nodes[1].toString().equals(GEO_LONGITUDE))
          longitude = Float.parseFloat(nodes[2].toString());

        if (nodes[1].toString().equals(WON_HAS_LOWER_PRICE_LIMIT))
          priceLower = Float.parseFloat(nodes[2].toString());
        if (nodes[1].toString().equals(WON_HAS_UPPER_PRICE_LIMIT))
          priceUpper = Float.parseFloat(nodes[2].toString());

      } catch (NumberFormatException e) {
        logger.log(Level.WARNING, "Error parsing numbers.", e);
      }

      if (nodes[1].toString().equals(WON_HAS_TAG))
        document.addField(FIELD_TAG, nodes[2].toString());

      nTriplesBuilder.append(String.format("%s %s %s .\n", nodes[0].toN3(), nodes[1].toN3(), nodes[2].toN3()));
    }

    public void parseTags(String text)
    {
      Matcher m = tagPattern.matcher(text);

      while (m.find())
        document.addField(FIELD_TAG, m.group());
    }

    @Override
    public void endDocument()
    {
      if (!isNeed) {
        logger.fine("Not a Need graph, skipping.");
        return;
      }

      if (this.documentUrl == null) {
        logger.warning("Could not determine document url by analyzing triples. Using " + this.provenance.getUri() + " as a fallback. This may cause problems.");
        this.documentUrl = this.provenance.getUri().toString();
      }
      this.document.addField(FIELD_URL, this.documentUrl);

      if (title != null)
        document.addField(FIELD_TITLE, title);

      if (description != null)
        document.addField(FIELD_DESCRIPTION, description);

      if (basicNeedType != null)
        document.addField(FIELD_BASIC_NEED_TYPE, basicNeedType);

      if (latitude > 0 && longitude > 0)
        document.addField(FIELD_LOCATION, latitude + "," + longitude);

      if (priceLower > -1)
        document.addField(FIELD_LOWERPRICE, priceLower);

      if (priceUpper > -1)
        document.addField(FIELD_UPPERPRICE, priceUpper);

      String nTriplesString = nTriplesBuilder.toString();

      if (nTriplesString.length() == 0) return;
      logger.fine("writing these triples: \n" + nTriplesString);
      this.document.addField(FIELD_NTRIPLE, nTriplesString);

      logger.info("writing document to siren/solr server, doc url is " + this.provenance.getUri().toString() + ", using resource url " + this.documentUrl);
      try {
        final UpdateRequest request = new UpdateRequest();
        request.add(this.document);
        request.process(this.solrServer);
      } catch (Exception e) {
        logger.log(Level.WARNING, "An error occurred when writing to solr server at " + solrServer.getBaseURL(), e);
      }
    }
  }

}
