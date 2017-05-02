package com.kmwllc.brigade.stage;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.Matches;
import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.matchers.ScoringMatch;
import uk.co.flax.luwak.matchers.ScoringMatcher;
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;

/**
 * Matches Luwak queries against processed {@link Document} and stores a result in
 * the document's field {@link #LUWAK_MATCHES_FIELD}.
 *
 * Following are specifics of the implementation:
 * 1. Only one document field can be used to configure matching against.
 * 2. All field values are concatenated into one string used for matching.
 * 3. {@link ScoringMatcher} is used and, as such, {@link ScoringMatch} is the match type.
 * 4. {@link Matches<ScoringMatch>} object is stored in {@link Document}s {@link #LUWAK_MATCHES_FIELD} field.
 */
public class LuwakQueryMatcher extends AbstractStage {

  public static final String FIELD_CONFIG_PARAMETER = "luwak.field";
  public static final String QUERIES_CONFIG_PARAMETER = "luwak.queries";
  public static final String LUWAK_MATCHES_FIELD = "luwak.matches";

  private Monitor monitor;
  private String field;

  @Override
  public void startStage(StageConfig config) {
    checkNotNull(config, "Null config");
    field = config.getStringParam(FIELD_CONFIG_PARAMETER);
    checkNotNull(field, "Missing required parameter %s", FIELD_CONFIG_PARAMETER);
    checkState(!field.isEmpty(), "Empty required parameter %s", FIELD_CONFIG_PARAMETER);
    Map<String, String> queries = config.getMapProperty(QUERIES_CONFIG_PARAMETER);
    checkNotNull(queries, "Missing required parameter %s", FIELD_CONFIG_PARAMETER);
    checkState(!queries.isEmpty(), "Empty required parameter %s", FIELD_CONFIG_PARAMETER);

    try {
      Monitor monitor = new Monitor(new LuceneQueryParser(field), new TermFilteredPresearcher());
      for (Map.Entry<String, String> queryEntry : queries.entrySet()) {
        MonitorQuery mq = new MonitorQuery(queryEntry.getKey(), queryEntry.getValue());
        monitor.update(mq);
      }
      this.monitor = monitor; // instance variable also serves as indicator of successful initialization
    } catch (Throwable t) {
      throwIfUnchecked(t);
    }
  }

  @Override
  public List<Document> processDocument(Document doc) {
    checkNotNull(doc, "Null document");
    checkNotNull(monitor, "Query monitor not initialized");

    try {
      InputDocument.Builder builder = InputDocument.builder(doc.getId());
      List<Object> values = doc.getField(field);
      if (values != null && !values.isEmpty()) {
        // TODO: is joining the fields into one "text" value the right thing to do
        String text = Joiner.on(" ").join(values);
        builder.addField(field, text, new StandardAnalyzer());
      }
      InputDocument inputdDoc = builder.build();
      Matches<ScoringMatch> matches = monitor.match(inputdDoc, ScoringMatcher.FACTORY);
      doc.setField(LUWAK_MATCHES_FIELD, matches);
    } catch (Throwable t) {
      throwIfUnchecked(t);
    }
    return ImmutableList.of(doc);
  }

  @Override
  public void stopStage() {
    try {
      this.monitor.close();
    } catch (Throwable t) {
      throwIfUnchecked(t);
    }
  }

  @Override
  public void flush() {
  }
}
