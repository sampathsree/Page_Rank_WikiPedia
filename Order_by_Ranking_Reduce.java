/**
 * Name: Sampath Sree Kumar K
 * Email-id: skolluru@uncc.edu
 * Studentid: 800887568
 */
package wiki.org;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Order_by_Ranking_Reduce extends Reducer<FloatWritable, Text, FloatWritable, Text> {

    static int i = 100;  

    public void reduce(FloatWritable pagerank, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      
    	float rank = pagerank.get();
        for (Text value : values){
            if(i>0){
//            	System.out.println("This is working");
            	context.write(new FloatWritable(rank), value);
            	i--;
            }
        }
    }
    /*
     * Sample Output
     * <1.6167021	q6><1.367239	q3><1.1405247	q4>
     */
}