package com.kmwllc.brigade.connector;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;

import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.document.Document;

//gdrive connector
public class GDriveConnector extends AbstractConnector {

  private ConnectorConfig config = null;
  private String directory = "data";
  private boolean interrupted = false;
  
  private static final String APPLICATION_NAME = "Google Drive API Java Quickstart";
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static final String TOKENS_DIRECTORY_PATH = "tokens";
  /**
   * Global instance of the scopes required by this quickstart.
   * If modifying these scopes, delete your previously saved tokens/ folder.
   */
  private static final List<String> SCOPES = Collections.singletonList(DriveScopes.DRIVE_METADATA_READONLY);
  private static final String CREDENTIALS_FILE_PATH = "/credentials.json";
  
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

  /**
   * Creates an authorized Credential object.
   * @param HTTP_TRANSPORT The network HTTP Transport.
   * @return An authorized Credential object.
   * @throws IOException If the credentials.json file cannot be found.
   */
  private static Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT) throws IOException {
      // Load client secrets.
      InputStream in = GDriveConnector.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
      if (in == null) {
          throw new FileNotFoundException("Resource not found: " + CREDENTIALS_FILE_PATH);
      }
      GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

      // Build flow and trigger user authorization request.
      GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
              HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
              .setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH)))
              .setAccessType("offline")
              .build();
      LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
      return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
  }

  
  @Override
  public void startCrawling() throws GeneralSecurityException, IOException {
    log.info("GDrive File Connector started.");
    // TODO: should we flush here immediately?
    
    // Build a new authorized API client service.
    final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
    Drive service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT))
            .setApplicationName(APPLICATION_NAME)
            .build();

    // Print the names and IDs for up to 10 files.
    FileList result = service.files().list()
            .setPageSize(10)
            .setFields("nextPageToken, files(id, name)")
            .execute();
    List<File> files = result.getFiles();
    if (files == null || files.isEmpty()) {
        System.out.println("No files found.");
    } else {
        System.out.println("Files:");
        for (File file : files) {
            System.out.printf("%s (%s)\n", file.getName(), file.getId());
        }
    }
  }

  @Override
  public void stopCrawling() {
    // TODO : this lifecycle is bogus.. should all be refactored so you don't have to think about it.
    // let the base class handle it.
    this.interrupted = true;
    state = ConnectorState.STOPPED;
  }

}
