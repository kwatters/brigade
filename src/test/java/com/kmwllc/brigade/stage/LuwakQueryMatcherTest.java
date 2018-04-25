package com.kmwllc.brigade.stage;

import com.google.common.collect.ImmutableMap;
import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.config.json.JsonStageConfig;
import com.kmwllc.brigade.document.Document;
import org.junit.Before;
import org.junit.Test;
import uk.co.flax.luwak.Matches;
import uk.co.flax.luwak.QueryMatch;
import uk.co.flax.luwak.matchers.ScoringMatch;

import java.util.ArrayList;

import static com.kmwllc.brigade.stage.LuwakQueryMatcher.*;
import static org.assertj.core.api.Assertions.assertThat;

public class LuwakQueryMatcherTest {

  private StageConfig config = new JsonStageConfig();
  private Document document = new Document("doc1");
  private LuwakQueryMatcher matcher = new LuwakQueryMatcher();

  @Before
  public void setUp() {
    config.setStringParam(FIELD_CONFIG_PARAMETER, "myField");
    config.setMapParam(QUERIES_CONFIG_PARAMETER, ImmutableMap.of("query1", "word1",
        "query2", "word2"));
    matcher.startStage(config);
  }

  @Test
  public void exactlyOneMatch() {
    document.setField("myField", "word1");
    matcher.processDocument(document);

    ArrayList<Object> docField = document.getField(LUWAK_MATCHES_FIELD);
    Matches<QueryMatch> match = (Matches<QueryMatch>)docField.get(0);
    assertThat(match.getMatchCount("doc1")).isEqualTo(1);
    assertThat(match.matches("query1", "doc1")).isNotNull();
  }

  @Test
  public void twoMatches() {
    document.setField("myField", "word1");
    document.addToField("myField", "word2");
    matcher.processDocument(document);

    ArrayList<Object> docField = document.getField(LUWAK_MATCHES_FIELD);
    Matches<QueryMatch> match = (Matches<QueryMatch>)docField.get(0);
    assertThat(match.getMatchCount("doc1")).isEqualTo(2);
    assertThat(match.matches("query1", "doc1")).isNotNull();
    assertThat(match.matches("query2", "doc1")).isNotNull();
  }

  @Test
  public void scoring() {
    document.setField("myField", "word1");
    document.addToField("myField", "word2");
    matcher.processDocument(document);

    ArrayList<Object> docField = document.getField(LUWAK_MATCHES_FIELD);
    Matches<ScoringMatch> match = (Matches<ScoringMatch>)docField.get(0);
    assertThat(match.getMatchCount("doc1")).isEqualTo(2);
    assertThat(match.matches("query1", "doc1").getScore()).isNotZero();
    assertThat(match.matches("query2", "doc1").getScore()).isNotZero();
  }

  @Test
  public void noMatches() {
    document.setField("myField", "wordX");
    matcher.processDocument(document);

    ArrayList<Object> docField = document.getField(LUWAK_MATCHES_FIELD);
    Matches<QueryMatch> match = (Matches<QueryMatch>)docField.get(0);
    assertThat(match.getMatchCount("doc1")).isEqualTo(0);
  }
}
