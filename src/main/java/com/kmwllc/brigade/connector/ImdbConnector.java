package com.kmwllc.brigade.connector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.kmwllc.brigade.config.ConnectorConfiguration;
import com.kmwllc.brigade.document.Document;

public class ImdbConnector extends AbstractConnector {

	private String moviesFile = "test_data/imdb/movies.list";
	private String actorsFile = "test_data/imdb/actors.list";
	private String actoressFile = "test_data/imdb/actors.list";

	@Override
	public void initialize(ConnectorConfiguration config) {

		moviesFile = config.getProperty("moviesFile", moviesFile);
		actorsFile = config.getProperty("actorsFile", actorsFile);
		actoressFile = config.getProperty("actoressFile", actoressFile);

		// TODO: move this call into the base class.
		setWorkflow(config.getWorkflow());


	}

	@Override
	public void start() throws InterruptedException {
		// TODO Auto-generated method stub

		try {
			processActors();
			processMovies();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 


	}

	private void processActors() throws InterruptedException, IOException, FileNotFoundException {
		BufferedReader fileReader = createBufferedReader(actorsFile);
		String line = null;
		while ((line = fileReader.readLine()) != null)  {  
			// read the file up to the start of the data
			System.out.println(line);
			if (line.startsWith("Name")) {
				// read one more line
				fileReader.readLine();
				break;
			}
		}		
		
		int actorId = 0;

		while ((line = fileReader.readLine()) != null)  {  

			// line = line.trim();
			if (line.isEmpty()) {
				continue;
			}

			
			// this line has the current person and move
			line = line.replaceAll("(\t)+", "\t");
			String[] parts = line.split("\t");
			if (parts.length <2) {
				// TODO: what causes this?
				continue;
			}
			String name = parts[0].trim();
			String movie= parts[1].trim();

			ArrayList<String> movieList = new ArrayList<String>();
			movieList.add(movie);
			// now we want to read up to the next blank line. these are all movies
			while (true) {
				line = fileReader.readLine();
				if (line == null || line.trim().isEmpty()) {
					break;
				}
				movieList.add(line.trim());
			}
			
			actorId++;
			Document actorDoc = new Document("actor_" + actorId);
			actorDoc.setField("name", name);
			for (String m : movieList) {
				actorDoc.addToField("movies", m);
			}
			actorDoc.setField("table", "actor");
			feed(actorDoc);
					
			
			
		}



	}

	private BufferedReader createBufferedReader(String filename) throws FileNotFoundException {
		File fileHandle = new File(filename);
		if (!fileHandle.exists()) {
			System.out.println("File does not exist.." + fileHandle.getAbsolutePath());
			return null;
		}
		BufferedReader br = new BufferedReader(new FileReader(fileHandle));
		return br;
	}

	private void processMovies() throws InterruptedException, IOException, FileNotFoundException {
		File moviesFileHandle = new File(moviesFile);
		if (!moviesFileHandle.exists()) {
			System.out.println("File does not exist.." + moviesFileHandle.getAbsolutePath());
			return;
		}
		// the file exists
		BufferedReader br = new BufferedReader(new FileReader(moviesFileHandle));

		String line = null;  
		while ((line = br.readLine()) != null)  {  
			// read the file up to the start of the data
			if (line.contains("===========")) {
				break;
			}
		}

		// we have read the header off now, the rest is data
		int movieId = 0;
		String docIdPrefix = "movie_";
		while ((line = br.readLine()) != null)  {  
			line = line.trim();
			if (line.isEmpty()) {
				continue;
			}
			movieId++;

			String docId = docIdPrefix + movieId;
			Document movieDoc = lineToMovieDoc(docId, line);
			feed(movieDoc);

		}


	}

	private Document lineToMovieDoc(String docId, String line) {
		// TODO Auto-generated method stub
		Document movieDoc = new Document(docId);

		int titleIndex = line.indexOf("(");
		int yearIndex = line.indexOf(")", titleIndex);

		String title = line.substring(0,titleIndex-1);
		movieDoc.setField("title", title);

		String year = line.substring(titleIndex+1, yearIndex);
		movieDoc.setField("year", year);

		movieDoc.setField("table", "movie");
		return movieDoc;
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub

	}

}
