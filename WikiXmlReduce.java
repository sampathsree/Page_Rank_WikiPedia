/**
 * Name: Sampath Sree Kumar K
 * Email-id: skolluru@uncc.edu
 * Studentid: 800887568
 */
 package wiki.org;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * 
 * @author sam
 * 
 */

public class WikiXmlReduce extends Reducer<Text, Text, Text, Text> {

	private float dampfactor;

	/*
	 * Sample Input <q0 q2> <q1 q1> <q1 q2>
	 */

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		dampfactor = context.getConfiguration().getFloat("dampfactor", 0);
		Float pg_rank = 1 - dampfactor;
		String pagerank = String.valueOf(pg_rank) + "\t";

		boolean first = true;
		if (!values.toString().isEmpty()) {
			for (Text value : values) {
				if (!first)
					pagerank += ";"; // Links are seperated by semi colon

				pagerank += value.toString();
				first = false;
			}
			context.write(key, new Text(pagerank));
		} else {
			context.write(key, new Text(pagerank + "" + ";"));
		}

	}

	/*
	 * Sample Output <q0 0.14999998 q2> <q1 0.14999998 q1;q2>
	 */
}