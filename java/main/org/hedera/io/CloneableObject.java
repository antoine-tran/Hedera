package org.hedera.io;

/** A typed version of Java Cloneable interface */
public interface CloneableObject<T> {

	/** override the fields of this object with values from the source */
	public void clone(T source);
}
