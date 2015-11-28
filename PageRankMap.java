/**
 * Name: Sampath Sree Kumar K
 * Email-id: skolluru@uncc.edu
 * Studentid: 800887568
 */
 package wiki.org;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * 
 * @author sam
 * 
 */

public class PageRankMap extends Mapper<LongWritable, Text, Text, Text> {

	/*
	 * Sample Input <q0 0.14999998 q2> <q1 0.14999998 q1;q2>
	 */

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		int pageTabIndex = value.find("\t");
		int rankTabIndex = value.find("\t", pageTabIndex + 1);

		String currentpage = Text.decode(value.getBytes(), 0, pageTabIndex);
		String currentpage_Rank = Text.decode(value.getBytes(), 0,
				rankTabIndex + 1);

		// Mark page as an Existing page (ignore red wiki-links)
		context.write(new Text(currentpage), new Text("exists"));
//		System.out.println(new Text(currentpage) + ", " + "exists");

		// Skip pages with no links.
		if (rankTabIndex == -1)
			return;

		String links = Text.decode(value.getBytes(), rankTabIndex + 1,
				value.getLength() - (rankTabIndex + 1));
		String[] allOtherPages = links.split(";");
		int totalLinks = allOtherPages.length;

		if (!(totalLinks == 0)) {
			for (String otherPage : allOtherPages) {
				Text pageRankTotalLinks = new Text(currentpage_Rank
						+ totalLinks);
				context.write(new Text(otherPage), pageRankTotalLinks);
//				System.out.println(new Text(otherPage) + ", "+ pageRankTotalLinks);
			}
		} else {
			String temp = "";
			// If it is zero it would result in / by zero Exception, so total
			// links = 1 if no outgoing links
			Text pageRankTotalLinks = new Text(currentpage_Rank + "1");
			context.write(new Text(temp), pageRankTotalLinks);
//			System.out.println(new Text(temp) + ", " + pageRankTotalLinks);
		}
		// Put the original links of the page for the reduce output
		context.write(new Text(currentpage), new Text("=" + links));
//		System.out.println(currentpage + ", =" + links);
	}

	/*
	 * sample output <q0, exists> // no links exist in this page <, q0
	 * 0.14999998 1> <q0, => <q1, exists> <q1, q1 0.14999998 2> <q2, q1 0.14999998
	 * 2> <q1, =q1;q2>
	 */
}