package org.hedera.io.etl;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.hedera.io.WikipediaLinkSnapshot;
import org.hedera.io.WikipediaRevisionHeader;
import org.hedera.io.WikipediaLinkSnapshot.Link;

public class WikipediaLinkExtractor implements
ETLExtractor<LongWritable, WikipediaLinkSnapshot, WikipediaRevisionHeader> {

	@Override
	public float check(WikipediaRevisionHeader curMeta,
			WikipediaRevisionHeader prevMeta) {		
		if (prevMeta == null || prevMeta.getLength() == 0) return 1f;		
		return (curMeta.getLength() - prevMeta.getLength()) / prevMeta.getLength();
	}

	/*@Override
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
		byte[] b = content.getData();
		int len = content.getLength();
		int i = 0;
		int start = -1;
		int end = -1;
		while (i < len - 3) {

			// we read every two characters, or 4 bytes at one time
			char c = (char) (((b[i] & 0xFF) << 8) + (b[i+1] & 0xFF));
			char d = (char) (((b[i+2] & 0xFF) << 8) + (b[i+3] & 0xFF));
			if (c == '[' && d == '[') {
				start = i;
			}
			else if (c == ']' && d == ']') {
				end = i;
				if (start > 0) {
					try {
						String text = new String(b,start,end, "UTF-8");
						String anchor = null;

						// skip empty links, special links
						if (text.length() > 0 && text.indexOf(":") == -1) {

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
							Link l = new Link(anchor, text);
							value.addLink(l);
						}
					}
					catch (Exception e) {
						Log.debug("No way: ", e);
					}
				}
				start = -1;
			}
			i += 2;
		}
	}*/

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
	}
}
