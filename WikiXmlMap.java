/**
 * Name: Sampath Sree Kumar K
 * Email-id: skolluru@uncc.edu
 * Studentid: 800887568
 */
 
package wiki.org;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 * @author sam
 * 
 */
public class WikiXmlMap extends Mapper<LongWritable, Text, Text, Text> {

	private static final Pattern outLinksPattern = Pattern.compile("\\[.+?\\]");


	
	/*
	 * Sample Input <title>q0</title> <text>[[q2]]</text> <title>q1</title>
	 * <text>[[q1]][[q2]]</text>
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if(value.getLength()==0)return;
		
		String[] titleAndText = getTitleAndText(value);

		String title = titleAndText[0];

		Text page = new Text(title.replace(' ', '_'));

		Matcher matcher = outLinksPattern.matcher(titleAndText[1]);
		boolean Flag = true;

		// Loop through the matched links in <text></text>
		while (matcher.find()) {
			String outgoinglinks = matcher.group();
			// Filter only wiki pages.
			// - some have [[realPage|linkName]], some single [realPage]
			// - some link to files or external pages.
			// - some link to paragraphs into other pages.
			outgoinglinks = getWikiPageFromLink(outgoinglinks);
			if (outgoinglinks == null || outgoinglinks.isEmpty())
				continue;
			
			Flag = false;
			// add valid outlinks to the map.
			context.write(page, new Text(outgoinglinks));
			System.out.println(page + ", " + outgoinglinks);
		}

		if (!matcher.find() && Flag == true && !(title=="")) {
			String outgoing = "";
			context.write(page, new Text(outgoing));
			System.out.println(page + ", " + outgoing);
		}
	}

	/*
	 * Sample Output <q0 q2> <q1 q1> <q1 q2> <q2 >
	 */

	private String getWikiPageFromLink(String outLink) {

		//int start = 2;
		int start = outLink.startsWith("[[") ? 2 : 1;
		int endLink = outLink.indexOf("]");

		int part = outLink.indexOf("#");
		if (part > 0) {
			endLink = part;
		}

		if(start==2){
			outLink = outLink.substring(start, endLink);
		outLink = outLink.replaceAll(" ", "_");
		return outLink;
		}else{
			return "";
		}
	}

	private String[] getTitleAndText(Text value)
			throws CharacterCodingException {
		String[] titleAndText = new String[2];

		int start = value.find("<title>");
		int end = value.find("</title>", start);
		start += 7; // add <title> length.

		titleAndText[0] = Text.decode(value.getBytes(), start, end - start);

		start = value.find("<text");
		start = value.find(">", start);
		end = value.find("</text>", start);
		start += 1;

		if (start == -1 || end == -1) {
			return new String[] { "", "" };
		}

		titleAndText[1] = Text.decode(value.getBytes(), start, end - start);

		return titleAndText;
	}

}