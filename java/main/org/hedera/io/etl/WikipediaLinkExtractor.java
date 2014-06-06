package org.hedera.io.etl;


import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.hedera.io.WikipediaLinkSnapshot;
import org.hedera.io.WikipediaRevisionHeader;
import org.hedera.io.WikipediaLinkSnapshot.Link;

public class WikipediaLinkExtractor implements
ETLExtractor<LongWritable, WikipediaLinkSnapshot, WikipediaRevisionHeader> {

	private static final Logger LOG = Logger.getLogger(WikipediaLinkExtractor.class);
	private static final byte[] OPEN_BRACKET = "[[".getBytes(StandardCharsets.UTF_8);
	private static final byte[] CLOSE_BRACKET = "]]".getBytes(StandardCharsets.UTF_8);

	@Override
	public float check(WikipediaRevisionHeader curMeta,
			WikipediaRevisionHeader prevMeta) {		
		if (prevMeta == null || prevMeta.getLength() == 0) return 1f;		
		return (curMeta.getLength() - prevMeta.getLength()) / prevMeta.getLength();
	}

	@Override
	public void extract(DataOutputBuffer content, WikipediaRevisionHeader meta,
			LongWritable key, WikipediaLinkSnapshot value) {

		// add meta-data
		key.set(meta.getPageId());
		value.setNamespace(meta.getNamespace());
		value.setPageId(meta.getPageId());
		value.setPageTitle(meta.getPageTitle());
		value.setParentId(meta.getParentId());
		value.setRevisionId(meta.getRevisionId());
		value.setTimestamp(meta.getRevisionId());

		// add content (here the list of links)	
		DataOutputBuffer linkBuffer = new DataOutputBuffer();
		byte[] bytes = content.getData();
		int len = content.getLength();
		int i = 0;

		// flag = 1: not see [[ or has passed ]] token
		// flag = 2: seen [[ but not ]] yet
		int flag = 1;
		try {
		for (int cursor = 0; cursor < len; cursor++) {
			byte b = bytes[cursor];
			if (flag == 1) {				
				if (b == OPEN_BRACKET[i]) {
					i++;					
				} else i = 0;
				if (i >= OPEN_BRACKET.length) {
					flag = 2;
					i = 0;
				}
			}
			else if (flag == 2) {
				if (b == CLOSE_BRACKET[i]) {
					i++;					
				} else i = 0;
				linkBuffer.write(b);
				if (i >= CLOSE_BRACKET.length) {						
					String linkText = new String(linkBuffer.getData(), 0,
							linkBuffer.getLength() - CLOSE_BRACKET.length,
							StandardCharsets.UTF_8);
					Link l = Link.convert(linkText, false);
					if (l != null) {
						value.addLink(l);
					}
					linkBuffer.reset();
					flag = 1;
					i = 0;					
				}
			}		
		}
		} catch (IOException e) {
			LOG.error("Error extracting link from revision: [" 
					+ value.getPageId() + ", rev: " + value.getRevisionId() + "]");
		}
	}

	/*public void extract(DataOutputBuffer content, WikipediaRevisionHeader meta,
			LongWritable key, WikipediaLinkSnapshot value) {

		// add meta-data
		key.set(meta.getPageId());
		value.setNamespace(meta.getNamespace());
		value.setPageId(meta.getPageId());
		value.setPageTitle(meta.getPageTitle());
		value.setParentId(meta.getParentId());
		value.setRevisionId(meta.getRevisionId());
		value.setTimestamp(meta.getRevisionId());

		// add content (here the list of links)	
		try {
			String s = new String(content.getData(), "UTF-8");
			int start = 0;
			while (true) {
				start = s.indexOf("[[", start);

				if (start < 0) {
					break;
				}

				int end = s.indexOf("]]", start);

				if (end < 0) {
					break;
				}

				String text = s.substring(start + 2, end);
				String anchor = null;

				// skip empty links
				if (text.length() == 0) {
					start = end + 1;
					continue;
				}

				// skip special links
				if (text.indexOf(":") != -1) {
					start = end + 1;
					continue;
				}

				// if there is anchor text, get only article title
				int a;
				if ((a = text.indexOf("|")) != -1) {
					anchor = text.substring(a + 1, text.length());
					text = text.substring(0, a);
				}

				if ((a = text.indexOf("#")) != -1) {
					text = text.substring(0, a);
				}

				// ignore article-internal links, e.g., [[#section|here]]
				if (text.length() == 0) {
					start = end + 1;
					continue;
				}

				if (anchor == null) {
					anchor = text;
				}
				value.addLink(new Link(anchor, text));
				start = end + 1;
			}
		}
		catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Weird encoding exception: ", e);
		}
	}*/
}
