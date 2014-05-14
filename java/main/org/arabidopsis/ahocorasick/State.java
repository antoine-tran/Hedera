package org.arabidopsis.ahocorasick;

import java.util.HashSet;
import java.util.Set;


/**
   A state represents an element in the Aho-Corasick tree.
 */


class State {


    // Arbitrarily chosen constant.  If this state ends up getting
    // deeper than THRESHOLD_TO_USE_SPARSE, then we switch over to a
    // sparse edge representation.  I did a few tests, and there's a
    // local minima here.  We may want to choose a more sophisticated
    // strategy.
    private static final int THRESHOLD_TO_USE_SPARSE = 3;

    private int depth;
    private EdgeList edgeList;
    private State fail;
    private Set outputs;

    public State(int depth) {
	this.depth = depth;
	if (depth > THRESHOLD_TO_USE_SPARSE)
	    this.edgeList = new SparseEdgeList();
	else
	    this.edgeList = new DenseEdgeList();
	this.fail = null;
	this.outputs = new HashSet();
    }


    public State extend(byte b) {
	if (this.edgeList.get(b) != null)
	    return this.edgeList.get(b);
	State nextState = new State(this.depth + 1);
	this.edgeList.put(b, nextState);
	return nextState;
    }


    public State extendAll(byte[] bytes) {
	State state = this;
	for (int i = 0; i < bytes.length; i++) {
	    if (state.edgeList.get(bytes[i]) != null)
		state = state.edgeList.get(bytes[i]);
	    else
		state = state.extend(bytes[i]);
	}
	return state;
    }


    /**
       Returns the size of the tree rooted at this State.  Note: do
       not call this if there are loops in the edgelist graph, such as
       those introduced by AhoCorasick.prepare().
     */
    public int size() {
	byte[] keys = edgeList.keys();
	int result = 1;
	for (int i = 0; i < keys.length; i++)
	    result += edgeList.get(keys[i]).size();
	return result;
    }


    public State get(byte b) {
	return this.edgeList.get(b);
    }


    public void put(byte b, State s) {
	this.edgeList.put(b, s);
    }

    public byte[] keys() {
	return this.edgeList.keys();
    }


    public State getFail() {
	return this.fail;
    }


    public void setFail(State f) {
	this.fail = f;
    }


    public void addOutput(Object o) {
	this.outputs.add(o);
    }


    public Set getOutputs() {
	return this.outputs;
    }
}
