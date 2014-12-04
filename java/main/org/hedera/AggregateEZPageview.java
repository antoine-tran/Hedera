package org.hedera;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import tuan.io.FileUtility;

/**
 * Aggregate the page views after normalizing titles using LocalEZPageview program
 *  
 * @author tuan
 *
 */
public class AggregateEZPageview {

	private static final DateTimeFormatter dtfMonth = DateTimeFormat
			.forPattern("YYYY-mm");
	private static final DateTimeFormatter dtfMonthPrinter = DateTimeFormat
			.forPattern("YYYYmm");
	/** Input files are supposed to be sorted 
	 * @throws IOException */
	public static void main(String[] args) throws IOException {
		
		DateTime month = dtfMonth.parseDateTime(args[2]);
		int dayOfMonth = month.dayOfMonth(	).getMaximumValue();

		int[] value = new int[dayOfMonth + 1];

		int monthAsInt = Integer.parseInt(dtfMonthPrinter.print(month));
		
		String prevTitle = null;
		FileWriter writer = new FileWriter(args[1]);
		Writer o = new BufferedWriter(writer);
		for (String line : FileUtility.readLines(args[0])) {
			if (line.isEmpty()) {
				continue;
			}
			int i = line.indexOf('\t');
			if (i < 0) {
				continue;
			}
			i = line.indexOf('\t',i+1);
			if (i < 0) {
				continue;
			}
			String titleAndMonth = line.substring(0,i);
			if (prevTitle != null && !prevTitle.equals(titleAndMonth)) {
				StringBuilder sb = new StringBuilder();
				sb.append(prevTitle);
				sb.append('\t');
				sb.append(monthAsInt);
				for (int item : value) {
					sb.append('\t');
					sb.append(item);
				}
				sb.append('\n');
				o.write(sb.toString());
				for (int j = 0; j < value.length; j++) {
					value[j] = 0;
				}
			}
			for (int j = 0, k = i, tmp = k; j < value.length; j++, k = tmp) {
				tmp = line.indexOf('\t',k+1);
				value[j] += Integer.parseInt(line.substring(k+1,tmp));
			}
			prevTitle = titleAndMonth;
		}
		if (prevTitle != null) {
			StringBuilder sb = new StringBuilder();
			sb.append(prevTitle);
			sb.append('\t');
			sb.append(monthAsInt);
			for (int item : value) {
				sb.append('\t');
				sb.append(item);
			}
			sb.append('\n');
			o.write(sb.toString());
			for (int j = 0; j < value.length; j++) {
				value[j] = 0;
			}
		}
		o.close();
	}
}
