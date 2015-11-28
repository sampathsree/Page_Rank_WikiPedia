/**
 * Name: Sampath Sree Kumar K
 * Email-id: skolluru@uncc.edu
 * Studentid: 800887568
 */
 package wiki.org;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import wiki.org.*;

public class WikiPageRank_Runner extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new WikiPageRank_Runner(), args);
		getInvertedIndex(args);
		System.exit(res);

	}
	
	public static void getInvertedIndex(String[] args){
		try {
			InvertedIndexer idx = new InvertedIndexer();

			String path = args[0];
			File[] f = new File(path).listFiles();
			idx.indexFile(f[0]);
			path = args[3];
			idx.search(Arrays.asList(path.split(",")));
		} catch (Exception e) {
			//e.printStackTrace();
		}
	}

	private static NumberFormat nf = new DecimalFormat("00");
	public int run(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(getConf());
		Path InputFilePath = new Path(args[0]);
		//Path OutputFilePath = new Path("wikidata/pagerankcalc/iter00");
		Path OutputFileDirectory = new Path(args[1]);
		if (fs.exists(OutputFileDirectory)) {
			fs.delete(OutputFileDirectory, true); // Remove Final Path for Job1
		}
		// Create a Map Reduce job to extract content from xml
		Job job1 = new Job(getConf(), "xmlExtract");
		job1.setJarByClass(this.getClass());
		// Input Mapper
		job1.setMapperClass(WikiXmlMap.class);
		FileInputFormat.addInputPath(job1, InputFilePath);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		// Output Reduce
		int numberofreducers  = (int) (1.75*2*job1.getNumReduceTasks());
		System.out.println(numberofreducers);
		job1.setNumReduceTasks(numberofreducers);
		job1.setReducerClass(WikiXmlReduce.class);
		FileOutputFormat.setOutputPath(job1, new Path(OutputFileDirectory+"/iter00"));
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.getConfiguration().set("dampfactor", String.valueOf("0.85F"));

		job1.waitForCompletion(true);

		String lastResultPath = null;
		boolean isCompleted = true;

		for (int runs = 0; runs < 10; runs++) {
			String inPath = OutputFileDirectory+"/iter" + nf.format(runs);
			lastResultPath = OutputFileDirectory+"/iter" + nf.format(runs + 1);

			isCompleted = runRankCalculation(inPath, lastResultPath);

			if (!isCompleted)
				return 1;
		}

		isCompleted = PageRankOrder(lastResultPath,
				OutputFileDirectory+"/result");

		if (isCompleted){
			FileStatus[] FilesList = fs.listStatus(OutputFileDirectory);
			int totalinputfiles = FilesList.length - 1;
			for(int i = 0; i<FilesList.length; i++ ){
				if(!FilesList[i].getPath().getName().equals("result")){
					fs.delete(new Path(OutputFileDirectory+"/"+FilesList[i].getPath().getName()),true);
				}
			}
			return 0;
		}
			
			
		return 1;
	}

	private boolean runRankCalculation(String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {

		Job rankCalculator = new Job(getConf(), "PageRankCalc");
		rankCalculator.setJarByClass(WikiPageRank_Runner.class);

		rankCalculator.setOutputKeyClass(Text.class);
		rankCalculator.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(rankCalculator, new Path(inputPath));
		FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));

		rankCalculator.getConfiguration().set("dampfactor",
				String.valueOf("0.85F"));
		int numberofreducers  = (int) (1.75*2*rankCalculator.getNumReduceTasks());
		rankCalculator.setNumReduceTasks(numberofreducers);
		rankCalculator.setMapperClass(PageRankMap.class);
		rankCalculator.setReducerClass(PageRankReduce.class);

		return rankCalculator.waitForCompletion(true);
	}

	private boolean PageRankOrder(String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {

		Job rankOrdering = new Job(getConf(), "rankOrdering");
		rankOrdering.setJarByClass(WikiPageRank_Runner.class);

		rankOrdering.setMapOutputKeyClass(FloatWritable.class);
		rankOrdering.setMapOutputValueClass(Text.class);
		rankOrdering.setOutputKeyClass(FloatWritable.class);
		rankOrdering.setOutputValueClass(Text.class);
		
		
		FileInputFormat.setInputPaths(rankOrdering, new Path(inputPath));
		FileOutputFormat.setOutputPath(rankOrdering, new Path(outputPath));

//		rankOrdering.setInputFormatClass(TextInputFormat.class);
//		rankOrdering.setOutputFormatClass(TextOutputFormat.class);
		
		rankOrdering.setSortComparatorClass(DescendingComparable.class);
		
		rankOrdering.setMapperClass(Order_by_Ranking.class);
		rankOrdering.setNumReduceTasks(1);
		rankOrdering.setReducerClass(Order_by_Ranking_Reduce.class);
		
		return rankOrdering.waitForCompletion(true);
	}

	public static class DescendingComparable extends WritableComparator {
		public DescendingComparable() {
			super(FloatWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			Float x1 = ByteBuffer.wrap(b1, s1, l1).getFloat();
			Float x2 = ByteBuffer.wrap(b2, s2, l2).getFloat();
			return x1.compareTo(x2) * -1;
		}

	}

}
