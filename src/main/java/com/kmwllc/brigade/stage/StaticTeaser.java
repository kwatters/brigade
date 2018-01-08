package com.kmwllc.brigade.stage;

import com.google.common.base.Strings;
import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by matt on 4/18/17.
 * 
 * Edited by Corey Clemente in 07/27/2017
 */
public class StaticTeaser extends AbstractStage {
	private List<String> inputFields = null;
    private String outputField = "teaser";
    private int targetLength = 200;
    private int minLength = 190;
    private String defaultDelimiter = "...";
    private String defaultTeaser = "Teaser not available.";
    private static Pattern border = Pattern.compile("\\W");

    @Override
    public void startStage(StageConfig config) {
    	inputFields = config.getListParam("inputFields");
        outputField = config.getStringParam("outputField", outputField);
        targetLength = config.getIntegerParam("length", targetLength);
        minLength = config.getIntegerParam("length", minLength);
        defaultDelimiter = config.getStringParam("defaultSeparator", defaultDelimiter);
        defaultTeaser = config.getStringParam("defaultTeaser", defaultTeaser);
    }
    
    // build teaser string from components from different fields
    private String buildTeaser(List<String> teaserComponents) {
    	return String.join(defaultDelimiter, teaserComponents);
    }

    @Override
    public List<Document> processDocument(Document doc) throws Exception {
    	// partial teaser components to be joined with delimiter
        List<String> teaserComponents = new ArrayList<String>();
        
        // store variables locally to avoid threading issues
        int originalTargetLength = targetLength;
        int remainingTargetLength = targetLength;

        for (String inputField : inputFields) {
	        if (doc.hasField(inputField) && !Strings.isNullOrEmpty(doc.getField(inputField).get(0).toString())) {
	        	
	        	// calculate max length remaining
	            String input = doc.getField(inputField).get(0).toString();
	            int maxLength = Math.min(remainingTargetLength, input.length()); // maximum length of teaser
	            String substring = input.substring(0, maxLength);
	
	            // build the teaser and append to components
	            if (substring.length() == input.length()) {
	                // ie. there's only one word and it's the length of the cutoff
	                teaserComponents.add(input);
	            } else {
	                // Find first word break and if exists break.
	                int i = maxLength;
	                while (i-- > 0 && !border.matcher(substring.substring(i, i + 1)).matches()) {
	                }
	                if (i > 0) {
	                    teaserComponents.add(substring.substring(0, i));
	                }
	            }
	            
	            // check if total teaser length hit minimum length
	            String currentTeaser = buildTeaser(teaserComponents);
	            if (currentTeaser.length() >= minLength) {
	            	break; // no need to add to teaser from another field
	            } else { // if not, update remaining length variables and search next field
	            	remainingTargetLength = originalTargetLength - currentTeaser.length();
	            }
	        }
        }
        
        // build final teaser and add to document
        String teaser = defaultTeaser; // in case no content found
        if (teaserComponents.size() != 0) { // content was found
        	teaser = buildTeaser(teaserComponents);
        }
        doc.addToField(outputField, teaser);
        return null;
    }

    @Override
    public void stopStage() {

    }

    @Override
    public void flush() {

    }
}
