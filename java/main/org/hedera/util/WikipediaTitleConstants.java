package org.hedera.util;

/** This class contains constants regarding titles of Wikipedia projects */
public class WikipediaTitleConstants {
	
	// The list of prefixes and suffixes which the pageview extractor should ignore
	public static final String[] ENWIKI_NON_ARTICLE_PREFIX = {
		"Category:","File:","Wikipedia:","Wikipedia/",
		"Wikipedia#","User:","Special:","Portal:",
		"Portal_talk:","Talk:","Help:","Template:",
		"/Template:","/Template_talk:","Translate:",
		"/Wikipedia_talk:","http://","https://",
		"//upload","/File:","simple:","hr:","hu:"};
	
	public static final String[] NON_ARTICLE_SUFFIX = {
		"HTML",".jpg",".html","HTML",".jpg",".txt",".TXT",
		".JPG",".gif",".GIF",".css",".CSS",".bmp",".BMP",
		".php",".PHP",".svg",".SVG",".OGG",".ogg",".ogv",".webm"
	};
	
	public static final String[] DEWIKI_NON_ARTICLE_PREFIX = {
		"Datei:","Benutzer:","Portal:","Kategorie:","Wikipedia:","Wikipedia/",
		"Wikipedia#","Spezial:","Diskussion:","Vorlage:","Hilfe:","simple:","hr:","hu:",
		"http://","https://","//upload",
	};
}
