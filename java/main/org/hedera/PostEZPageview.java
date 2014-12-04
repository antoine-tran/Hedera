package org.hedera;

/**
 * Post-processing of ez pageviews:
 * 1. Normalize titles using Wiki standard
 * 2. Aggregate the time series of the same normalized title
 * 3. For titles not matching Wikipedia pages:
 * 3.1. Strip out heading and trailing non-alphabets (maximum 2 characters per each end)
 * 3.2. Match again Wikipedia pages
 * 3.3. Aggregate the time series by stripped titles that match
 * 
 * 4. Resolving redirects
 *  
 * @author tuan
 *
 */
public class PostEZPageview {

	public static void main(String[] args) {
		
		// Input 1: Map of Wikipedia standardized title - id
		String wikiMapFile = args[0];
		
		// Input 2: Time series file
		String tsFile = args[1];
		
		// Input 3: Redirect file
		String redirectFile = args[2];
			
		String output = args[3];
		
		
	}
}
