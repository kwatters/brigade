package com.kmwllc.brigade.concurrency;

import com.google.common.base.Strings;
import com.kmwllc.brigade.document.Document;

import java.io.*;
import java.util.*;

public class DumpDocReader {

  public List<Document> read(File file) {
    List<Document> docs = new ArrayList<>();

    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String s;
      Map<String, String> curr = null;
      while ((s = br.readLine()) != null) {
        if (s.length() > 0) {
          if (s.startsWith("///")) {
            if (curr != null) {
              Document doc = convertMap(curr);
              docs.add(doc);
            }
            curr = new HashMap<>();
          } else {
            String[] split = s.split(":");
            int numTrailingColon = split.length > 1 ? numTrailingColumns(s) : 0;

            String[] allButFirst = new String[split.length - 1];
            System.arraycopy(split, 1, allButFirst, 0, allButFirst.length);
            if (split.length >= 2 && !allEmpty(allButFirst)) {
              // Put ':'s back in value string
              List<String> val = Arrays.asList(split);
              val = val.subList(1, val.size());
              String valString = String.join(":", val);
              if (numTrailingColon > 0) {
                for (int i = 0; i < numTrailingColon; i++) {
                  valString += ":";
                }
              }
              // HACK ALERT
              valString = valString.replace("--linebreak--", "\n");
              curr.put(split[0], valString);
            }
          }
        }
      }

      Document doc = convertMap(curr);
      docs.add(doc);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return docs;
  }

  private boolean allEmpty(String[] ss) {
    for (String s : ss) {
      if (!Strings.isNullOrEmpty(s)) {
        return false;
      }
    }
    return true;
  }

  private int numTrailingColumns(String s) {
    int num = 0;
    while (s.endsWith(":")) {
      num++;
      s = s.substring(0, s.length() - 1);
    }
    return num;
  }

  private Document convertMap(Map<String, String> map) {
    String id = map.get("id");
    Document doc = new Document(id);
    for (Map.Entry<String, String> e : map.entrySet()) {
      doc.setField(e.getKey(), e.getValue());
    }
    return doc;
  }
}

