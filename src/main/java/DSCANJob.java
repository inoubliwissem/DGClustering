

import org.apache.giraph.GiraphRunner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


public class DSCANJob 
{
	public static void main(String[] args) throws Exception {
	    
                long startTime = System.nanoTime();
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
		GiraphConfiguration conf = new GiraphConfiguration();
		GiraphRunner runner = new GiraphRunner();
		runner.setConf(conf);

		System.exit(ToolRunner.run(runner, new String[]{
				DSCAN.class.getName(),
				"-vip", args[0],
				"-vif", "InputFormat",
				"-vof", "OutputFormat",
				"-op", args[1]+dateFormat.format(new Date()),
				"-w", "1",
				"-ca", "mapred.job.tracker=local",
				"-ca", "giraph.SplitMasterWorker=false",
				"-ca", "giraph.useSuperstepCounters=false"}));

              	long endTime   = System.nanoTime();
		long totalTime = (endTime - startTime)/1000;
		System.out.println("Running time is "+totalTime);


	}
	 
}
