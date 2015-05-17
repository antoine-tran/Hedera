package org.hedera.util;

import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;

import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;

/** Utility for handling mediaWiki syntax */
public class MediaWikiProcessor {

	private static final Pattern REF = Pattern.compile("<ref>.*?</ref>");

	private static final Pattern LANG_LINKS = Pattern.compile("\\[\\[[a-z\\-]+:[^\\]]+\\]\\]");
	private static final Pattern DOUBLE_CURLY = Pattern.compile("\\{\\{.*?\\}\\}");

	private static final Pattern URL = Pattern.compile("http://[^ <]+"); // Note, don't capture
	// possible HTML tag

	private static final Pattern HTML_TAG = Pattern.compile("<[^!][^>]*>"); // Note, don't capture
	// comments
	private static final Pattern HTML_COMMENT = Pattern.compile("<!--.*?-->", Pattern.DOTALL);

	private WikiModel wikiModel;
	private PlainTextConverter textConverter;

	public MediaWikiProcessor() {
		wikiModel = new WikiModel("", "");
	    textConverter = new PlainTextConverter();
	}
	
	/** Extract the content from the mark-up text. This code is re-distributed from Cloud9,
	 * class {@link edu.umd.cloud9.collection.wikipedia.WikipediaPage}*/
	public String getContent(String raw) {
		String s = LANG_LINKS.matcher(raw).replaceAll(" ");

	    wikiModel.setUp();
	    s = wikiModel.render(textConverter, s);
	    wikiModel.tearDown();

	    // The way the some entities are encoded, we have to unescape twice.
	    s = StringEscapeUtils.unescapeHtml(StringEscapeUtils.unescapeHtml(s));

	    s = REF.matcher(s).replaceAll(" ");
	    s = HTML_COMMENT.matcher(s).replaceAll(" ");

	    // Sometimes, URL bumps up against comments e.g., <!-- http://foo.com/-->
	    // Therefore, we want to remove the comment first; otherwise the URL pattern might eat up
	    // the comment terminator.
	    s = URL.matcher(s).replaceAll(" ");
	    s = DOUBLE_CURLY.matcher(s).replaceAll(" ");
	    s = HTML_TAG.matcher(s).replaceAll(" ");

	    return s;
	}
}
