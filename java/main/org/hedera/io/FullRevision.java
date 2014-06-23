package org.hedera.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** 
 * A full revision that, besides Revision header and text,
 * stores also user and comment info
 */
public class FullRevision extends Revision {

	private String user;
	
	private long userId = -1;
	
	private String comment;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);	
		user = in.readUTF();
		userId = in.readLong();
		comment = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeUTF(user);
		out.writeLong(userId);
		out.writeUTF(comment);
	}
	
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public long getUserId() {
		return userId;
	}
	public void setUserId(long userId) {
		this.userId = userId;
	}
	public String getComment() {
		return comment;
	}
	public void setComment(String comment) {
		this.comment = comment;
	}

	// Read the contributor in the raw form:
    // <username>TerriersFan</username>
    // <id>1611404</id>
    // and extract info accordingly
	public void loadContributor(String raw) {		
		int i = raw.indexOf("<username>");
		if (i >= 0) {
			int j = raw.indexOf("</username>", i + 10);
			if (j >= 0) {
				user = raw.substring(i + 10, j);
			} else user = "";			
		}
		i = raw.indexOf("<id>");
		if (i >= 0) {
			int j = raw.indexOf("</id>", i + 4);
			if (j >= 0) {
				try {
					userId = Long.parseLong(raw.substring(i + 4, j));
				}
				catch (NumberFormatException e) {
					userId = -1;
				}
			} else userId = -1;			
		}
	}
	
	
}
