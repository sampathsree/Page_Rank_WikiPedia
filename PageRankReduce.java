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

public class PageRankReduce extends Reducer<Text, Text, Text, Text> {

	private float damping;

	/*
	 * Sample Input q0, exists // No links exist in this page , q0 0.14999998 1
	 * q0, = q1, exists q1, q1 0.14999998 2 q2, q1 0.14999998 2 q1, =q1;q2
	 */

	public void reduce(Text page, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		damping = context.getConfiguration().getFloat("dampfactor", 0);
		boolean isExistingWikiPage = false;
		String[] split;
		float sum_PageRanks = 0;
		String links = "";
		String pageWithRank;

		// For each otherPage:
		// - check control characters
		// - calculate pageRank share <rank> / count(<links>)
		// - add the share to sumShareOtherPageRanks
		for (Text value : values) {
			pageWithRank = value.toString();

			if (pageWithRank.equals("exists")) {
				isExistingWikiPage = true;
				continue;
			}

			if (pageWithRank.startsWith("=")) {
				links = "\t" + pageWithRank.substring(1);
				continue;
			}

			split = pageWithRank.split("\\t");

			float pageRank = Float.valueOf(split[1]);
			int countOutLinks = Integer.valueOf(split[2]);

			sum_PageRanks += (pageRank / countOutLinks);
		}

		if (!isExistingWikiPage)
			return;
		float newRank = damping * sum_PageRanks + (1 - damping);
		if(!page.toString().equals(""))
			context.write(page, new Text(newRank + links));
	}

	/*
	 * Sample Output
	 */
}