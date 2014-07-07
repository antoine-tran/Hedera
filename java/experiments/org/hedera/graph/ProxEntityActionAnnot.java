package org.hedera.graph;

import org.apache.hadoop.util.Tool;

import tuan.hadoop.conf.JobConfig;

/** This tool builds the annotation of entities for each
 * document vector based on the proximity heuristics
 * 
 * - Input: anchor / entity mapping, seed entities, proximity 
 * distance, entity repacked id list, repacked doc id list,
 * dictionary
 * 
 * - Output: csv file
 * [entity ID] TAB [word ID] TAB [timestamp]  
 * - For every doc vector, if the anchor is found,
 * then all target entities are annotated for the doc 
 * 
 * - If the seed word is specified and the proximity window
 * is known, the */
public class ProxEntityActionAnnot extends JobConfig implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
