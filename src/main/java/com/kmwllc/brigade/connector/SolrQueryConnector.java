package com.kmwllc.brigade.connector;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.slf4j.Logger;

import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;

public class SolrQueryConnector extends AbstractConnector {

	public final static Logger log = LoggerFactory.getLogger(SolrQueryConnector.class.getCanonicalName());
	private String solrUrl = null;
	private String queryString = "*:*";
	private int numRows = 10000;
	private Map<String,String> params = null;
	
	@Override
	public void setConfig(ConnectorConfig config) {
		solrUrl = config.getStringParam("solrUrl", solrUrl);

		numRows = config.getIntegerParam("numRows", numRows);
		queryString = config.getStringParam("queryString", queryString);
		Map<String,String> params = config.getMapParam("params");
	}

	@Override
	public void initialize() {
	}
	
	@Override
	public void startCrawling() throws IOException {
		HashSet<String> blackListFields = new HashSet<String>();
		blackListFields.add("_version_");
		HttpSolrClient client = new HttpSolrClient.Builder().withBaseSolrUrl(solrUrl).build();
		SolrQuery query = new SolrQuery(queryString);
		query.set("rows", numRows);
		if (params != null) {
			for (String key : params.keySet()) {
				query.add(key,params.get(key));
			}
		}
		QueryResponse qRes = null;
		try {
			qRes = client.query(query);
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		// TODO: iterate the end of the results.
		for (SolrDocument sDoc : qRes.getResults()) {
			// now i want to feed this as a brigade document.
			// the solr doc id.
			Document bDoc = new Document(sDoc.getFirstValue("id").toString());
			for (String fieldName : sDoc.getFieldNames()) {
				if (blackListFields.contains(fieldName)) {
					continue;
				}
				Collection<Object> objs = sDoc.getFieldValues(fieldName);
				for (Object o : objs) {
					bDoc.addToField(fieldName, o);
				}
			}
			// now set some static values TODO: remove this! use stage!
			feed(bDoc);
		}
		try {
			flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void stopCrawling() {
		// I guess we should do this here?
	}

}