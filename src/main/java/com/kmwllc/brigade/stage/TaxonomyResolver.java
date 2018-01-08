package com.kmwllc.brigade.stage;

import com.google.common.base.Strings;
import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by matt on 4/4/17.
 */
public class TaxonomyResolver extends AbstractStage {

    private static class Edge {
        String source;
        String target;
        Edge(String source, String target) {
            this.source = source;
            this.target = target;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Edge edge = (Edge) o;

            if (source != null ? !source.equals(edge.source) : edge.source != null) return false;
            return target != null ? target.equals(edge.target) : edge.target == null;
        }

        @Override
        public int hashCode() {
            int result = source != null ? source.hashCode() : 0;
            result = 31 * result + (target != null ? target.hashCode() : 0);
            return result;
        }
    }
    private final static Logger log = LoggerFactory.getLogger(TaxonomyResolver.class.getCanonicalName());

    private String inputField;
    private String outputField;
    private String delimiter;
    private String termPrefix;
    private String parentPrefix;

    @Override
    public void startStage(StageConfig config) {
        inputField = config.getStringParam("inputField");
        outputField = config.getStringParam("outputField");
        delimiter = config.getStringParam("delimiter");
        termPrefix = config.getStringParam("termPrefix");
        parentPrefix = config.getStringParam("parentPrefix");
       // log.info("Delimiter is {}", delimiter);
    }

    @Override
    public List<Document> processDocument(Document doc) {
        if (!doc.hasField(inputField)) {
            return null;
        }

        DefaultDirectedGraph<String, DefaultEdge> g = new DefaultDirectedGraph<>(DefaultEdge.class);

        // Assemble the graph of terms;
        // Collect vertices and edges in case specified out of order.
        // Edges can't be created if vertex not previously defined.
        Set<String> allVertices = new HashSet<>();
        Set<Edge> allEdges = new HashSet<>();

        for (Object o : doc.getField(inputField)) {
            String s = (String) o;

            String[] splits = s.split(delimiter);

            String term = null, parent = null;
            for (String ss : splits) {
                if (Strings.isNullOrEmpty(ss)) {
                    continue;
                }
                if (ss.startsWith(termPrefix)) {
                    term = ss.substring(termPrefix.length());
                } else if (ss.startsWith(parentPrefix)) {
                    parent = ss.substring(parentPrefix.length());
                }
                if (!Strings.isNullOrEmpty(term)) {
                    allVertices.add(term);
                }

                if (!Strings.isNullOrEmpty(parent)) {
                    allVertices.add(parent);
                    allEdges.add(new Edge(parent, term));
                }
            }
        }

        for (String v : allVertices) {
            g.addVertex(v);
        }
        for (Edge e : allEdges) {
            g.addEdge(e.source, e.target);
        }

        // Now we can collect all paths from root nodes
        for (String v : g.vertexSet()) {
            if (g.incomingEdgesOf(v).size() == 0) {
                String path = "";
                addPaths(g, path, v, doc);
            }
        }

        return null;
    }

    private void addPaths(DirectedGraph<String, DefaultEdge> g, String path, String vertex, Document doc) {
        if (path.length() == 0) {
            path = vertex;
        } else {
            path = String.format("%s/%s", path, vertex);
        }
        doc.addToField(outputField, path);
        // Recur over downstream vertices
        for (DefaultEdge de : g.outgoingEdgesOf(vertex)) {
            addPaths(g, path, g.getEdgeTarget(de), doc);
        }
    }

    @Override
    public void stopStage() {

    }

    @Override
    public void flush() {

    }
}
