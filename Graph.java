import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Tagged implements Writable {
    public boolean tag;                // true for a graph vertex, false for distance
    public int distance;               // the distance from the starting vertex
    public Vector<Integer> following;  // the vertex neighbors

    Tagged () { tag = false; }
    Tagged ( int d ) { tag = false; distance = d; }
    Tagged ( int d, Vector<Integer> f ) { tag = true; distance = d; following = f; }

    public void write ( DataOutput out ) throws IOException {
        out.writeBoolean(tag);
        out.writeInt(distance);
        if (tag) {
            out.writeInt(following.size());
            for ( int i = 0; i < following.size(); i++ )
                out.writeInt(following.get(i));
        }
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readBoolean();
        distance = in.readInt();
        if (tag) {
            int n = in.readInt();
            following = new Vector<Integer>(n);
            for ( int i = 0; i < n; i++ )
                following.add(in.readInt());
        }
    }
}

public class Graph {
    static int start_id = 14701391;
    static int max_int = Integer.MAX_VALUE;

    /* ... */
	public static class MyMapper extends Mapper<Object,Text,IntWritable,IntWritable> 
      {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException 
	{
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int x = s.nextInt();
            int y = s.nextInt();
            context.write(new IntWritable(y),new IntWritable(x));
            s.close();
        }
    }
  	public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,Tagged> {
        @Override
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            Vector<Integer> following = new Vector() ;
            for (IntWritable v: values) {
                following.add(v.get());
            };
	    if(key.get() == start_id || key.get() == 1)
	    {
	      context.write(key,new Tagged(0,following));
	    }
	    else
            context.write(key,new Tagged(max_int,following));
        }
    }
	
     public static class MyMapper2 extends Mapper<IntWritable,Tagged,IntWritable,Tagged> 
      {
        @Override
        public void map ( IntWritable key, Tagged value, Context context )
                        throws IOException, InterruptedException 
	{
           
            context.write(key,value);
	     if(value.distance < max_int)
		{
		   for(int id : value.following)
                    {
                      context.write(new IntWritable(id), new Tagged(value.distance + 1));
                    }
		}

        }
    }
  	public static class MyReducer2 extends Reducer<IntWritable,Tagged,IntWritable,Tagged> {
        @Override
        public void reduce ( IntWritable key, Iterable<Tagged> values, Context context )
                           throws IOException, InterruptedException {
            int min = max_int;
             Vector<Integer> following = new Vector() ;
            for (Tagged v: values) {
                if(v.distance < min)
		{
		  min = v.distance ;
		}
              if(v.tag)
		{
		  following = v.following ;
		}
            };
            context.write(key,new Tagged(min,following));
        }
    }
   public static class MyMapper3 extends Mapper<IntWritable,Tagged,IntWritable,IntWritable> 
      {
        @Override
        public void map ( IntWritable key, Tagged value, Context context )
                        throws IOException, InterruptedException 
	{
           
	     if(value.distance < max_int)
		{
		  context.write(key, new IntWritable(value.distance));
		}

        }
    }

    public static void main ( String[] args ) throws Exception {
        int iterations = 5;
        Job job = Job.getInstance();
        /* ... First Map-Reduce job to read the graph */
        /* ... */
        job.setJobName("Job1");
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Tagged.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"0"));
        job.waitForCompletion(true);
        for ( short i = 0; i < iterations; i++ ) {
           Job job2 = Job.getInstance();
            /* ... Seobnd Map-Reduce job to calculate shortest distance */
            /* ... */
                job2.setJobName("Job2");
        	job2.setJarByClass(Graph.class);
       		job2.setOutputKeyClass(IntWritable.class);
       		job2.setOutputValueClass(Tagged.class);
        	job2.setMapOutputKeyClass(IntWritable.class);
        	job2.setMapOutputValueClass(Tagged.class);
        	job2.setMapperClass(MyMapper2.class);
        	job2.setReducerClass(MyReducer2.class);
        	job2.setInputFormatClass(SequenceFileInputFormat.class);
        	job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            	FileInputFormat.setInputPaths(job2,new Path(args[1]+i));
            	FileOutputFormat.setOutputPath(job2,new Path(args[1]+(i+1)));
            	job2.waitForCompletion(true);
        }
        Job job3 = Job.getInstance();
        /* ... Last Map-Reduce job to output the distances */
        /* ... */
	   	job3.setJobName("Job3");
         	job3.setJarByClass(Graph.class);
		job3.setOutputKeyClass(IntWritable.class);
                job3.setOutputValueClass(IntWritable.class);
        	job3.setMapOutputKeyClass(IntWritable.class);
        	job3.setMapOutputValueClass(IntWritable.class);
        	job3.setMapperClass(MyMapper3.class);
        	job3.setInputFormatClass(SequenceFileInputFormat.class);
        	job3.setOutputFormatClass(TextOutputFormat.class);
        	FileInputFormat.setInputPaths(job3,new Path(args[1]+iterations));
        	FileOutputFormat.setOutputPath(job3,new Path(args[2]));
        	job3.waitForCompletion(true);
    }
}
