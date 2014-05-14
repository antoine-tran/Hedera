package org.arabidopsis.ahocorasick;

import java.util.Iterator;


/**
   <p>An implementation of the Aho-Corasick string searching
   automaton.  This implementation of the <a
   href="http://portal.acm.org/citation.cfm?id=360855&dl=ACM&coll=GUIDE"
   target="_blank">Aho-Corasick</a> algorithm is optimized to work
   with bytes.</p>

   <p>
   Example usage:
   <code><pre>
       AhoCorasick tree = new AhoCorasick();
       tree.add("hello".getBytes(), "hello");
       tree.add("world".getBytes(), "world");
       tree.prepare();

       Iterator searcher = tree.search("hello world".getBytes());
       while (searcher.hasNext()) {
           SearchResult result = searcher.next();
           System.out.println(result.getOutputs());
           System.out.println("Found at index: " + result.getLastIndex());
       }
   </pre></code>
   </p>

   <h2>Recent changes</h2>
   <ul>

   <li>Per user request from Carsten Kruege, I've
   changed the signature of State.getOutputs() and
   SearchResults.getOutputs() to Sets rather than Lists.
   </li>

   </ul>
 */
public class AhoCorasick {
    private State root;
    private boolean prepared;

    public AhoCorasick() {
	this.root = new State(0);
	this.prepared = false;
    }



    /**
       Adds a new keyword with the given output.  During search, if
       the keyword is matched, output will be one of the yielded
       elements in SearchResults.getOutputs().
     */
    public void add(byte[] keyword, Object output) {
	if (this.prepared)
	    throw new IllegalStateException
		("can't add keywords after prepare() is called");
	State lastState = this.root.extendAll(keyword);
	lastState.addOutput(output);
    }



    /**
       Prepares the automaton for searching.  This must be called
       before any searching().
     */
    public void prepare() {
	this.prepareFailTransitions();
	this.prepared = true;
    }




    /**
       Starts a new search, and returns an Iterator of SearchResults.
     */
    public Iterator search(byte[] bytes) {
	return new Searcher(this, this.startSearch(bytes));
    }








    /** DANGER DANGER: dense algorithm code ahead.  Very order
	dependent.  Initializes the fail transitions of all states
	except for the root.
    */
    private void prepareFailTransitions() {
	Queue q = new Queue();
	for(int i = 0; i < 256; i++)
	    if (this.root.get((byte) i) != null) {
		this.root.get((byte) i).setFail(this.root);
		q.add(this.root.get((byte) i));
	    }
	this.prepareRoot();
	while (! q.isEmpty()) {
	    State state = q.pop();
	    byte[] keys = state.keys();
	    for (int i = 0; i < keys.length; i++) {
		State r = state;
		byte a = keys[i];
		State s = r.get(a);
		q.add(s);
		r = r.getFail();
		while (r.get(a) == null)
		    r = r.getFail();
		s.setFail(r.get(a));
		s.getOutputs().addAll(r.get(a).getOutputs());
	    }
	}
    }


    /** Sets all the out transitions of the root to itself, if no
	transition yet exists at this point.
    */
    private void prepareRoot() {
	for(int i = 0; i < 256; i++)
	    if (this.root.get((byte) i) == null)
		this.root.put((byte) i, this.root);
    }



    /**
       Returns the root of the tree.  Package protected, since the
       user probably shouldn't touch this.
     */
    State getRoot() {
	return this.root;
    }

    

    /**
       Begins a new search using the raw interface.  Package protected.
     */
    SearchResult startSearch(byte[] bytes) {
	if (! this.prepared)
	    throw new IllegalStateException
		("can't start search until prepare()");
	return continueSearch
	    (new SearchResult(this.root, bytes, 0));
    }



    /**
       Continues the search, given the initial state described by the
       lastResult.  Package protected.
     */
    SearchResult continueSearch(SearchResult lastResult) {
	byte[] bytes = lastResult.bytes;
	State state = lastResult.lastMatchedState;
	for (int i = lastResult.lastIndex; i < bytes.length; i++) {
	    byte b = bytes[i];
	    while (state.get(b) == null)
		state = state.getFail();
	    state = state.get(b);
	    if (state.getOutputs().size() > 0)
		return new SearchResult(state, bytes, i+1);
	}
	return null;
    }



}
