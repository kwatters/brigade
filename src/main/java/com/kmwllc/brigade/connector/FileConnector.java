package com.kmwllc.brigade.connector;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;

import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.document.Document;

// TODO: make this the base class for everything that deals with files on a file system (or vfs)
public class FileConnector extends AbstractConnector implements FileVisitor<Path>{

  private ConnectorConfig config = null;
  private String directory = "data";
  private boolean interrupted = false;

  @Override
  public void setConfig(ConnectorConfig config) {
    // TODO Auto-generated method stub
    this.config = config;
  }

  @Override
  public void initialize() {
    // TODO: support a list of directories and wildcard includes/excludes
    directory = config.getStringParam("directory", directory);
    this.workflowName = config.getStringParam("workflowName", workflowName);
  }

  @Override
  public void startCrawling() {
    state = ConnectorState.RUNNING;
    Path startPath = Paths.get(directory);
    try {
      Files.walkFileTree(startPath, this);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    log.info("File Connector finished walking the tree.");
    // TODO: should we flush here immediately?
    state = ConnectorState.STOPPED;
  }

  @Override
  public void stopCrawling() {
    // TODO : this lifecycle is bogus.. should all be refactored so you don't have to think about it.
    // let the base class handle it.
    this.interrupted = true;
    state = ConnectorState.STOPPED;
  }

  @Override
  public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
    return FileVisitResult.CONTINUE;
  }

  @Override
  public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
    System.out.println(file);
    if (interrupted) {
      state = ConnectorState.INTERRUPTED;
      return FileVisitResult.TERMINATE;
    }
    String docId = getDocIdPrefix() + file.toFile().getAbsolutePath();
    Document doc = new Document(docId);
    doc.setField("last_modified", attrs.lastModifiedTime());
    doc.setField("created_date", attrs.creationTime());
    doc.setField("filename", file.toFile().getAbsolutePath());
    doc.setField("size", attrs.size());
    // TODO: potentially add a byte array of the file
    // or maybe an input stream or other handle to the file.
    feed(doc);
    return FileVisitResult.CONTINUE;
  }

  @Override
  public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
    throw exc;
  }

  @Override
  public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
    if (exc != null) {
      throw exc;
    }
    return FileVisitResult.CONTINUE;

  }



}
