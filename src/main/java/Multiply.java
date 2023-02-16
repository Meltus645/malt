import java.io.*;
import java.util.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Elem implements Writable{
    public short tag;
    public int index;
    public double value; 

    Elem(){index =0; value =0.0; tag =0;}
    Elem(short t, int i, double v){index =i; value =v; tag =t;}

    @Override
    public void readFields(DataInput reader)  throws IOException{ index =reader.readInt(); tag =reader.readShort(); value =reader.readDouble(); }

    @Override
    public void write(DataOutput writer) throws IOException{ writer.writeInt(index); writer.writeShort(tag); writer.writeDouble(value); }
}

class Pair implements WritableComparable<Pair> {
    public int i;
    public int j; 

    Pair () {}
    Pair ( int i, int j) { this.i = i; this.j = j;}

    public void readFields(DataInput reader) throws IOException{ i =reader.readInt(); j =reader.readInt();}

    public void write(DataOutput writer) throws IOException{ writer.writeInt(i);  writer.writeInt(j); }

    public int compareTo(Pair pair){ return (i ==pair.i? 0: (i < pair.i? -1: 1)); }

    @Override
    public String toString() { return i + " " + j + " ";}

}

public class Multiply extends Configured implements Tool{  
    public static class MatrixMMapper extends Mapper<Object, Text, IntWritable, Elem>{

        @Override
        public void  map(Object key, Text line, Context context) throws IOException, InterruptedException { 
            Scanner reader =new Scanner(line.toString());
            reader.useDelimiter(",");
            int i =reader.nextInt();
            int j =reader.nextInt();
            double v =reader.nextDouble();
            context.write(new IntWritable(j), new Elem((short)0, i, v));
        }
    } 

    public static class MatrixNMapper extends Mapper<Object, Text, IntWritable, Elem>{

        @Override
        public void map(Object key, Text line, Context context) throws IOException, InterruptedException{ 
            Scanner reader =new Scanner(line.toString());
            reader.useDelimiter(",");
            int i =reader.nextInt();
            int j =reader.nextInt();
            double v =reader.nextDouble();
            context.write(new IntWritable(i), new Elem((short)1, j, v));
        }
    } 

    public static class MatrixMxMatrixNReducer extends Reducer<IntWritable, Elem, Pair, DoubleWritable> { 

        @Override
        public void reduce(IntWritable index, Iterable<Elem> values, Context context) throws IOException, InterruptedException {
            ArrayList<Elem> A =new ArrayList<Elem>(); 
            ArrayList<Elem> B =new ArrayList<Elem>();  
            Configuration conf = context.getConfiguration();

            // check item tag (t) values and append either to A or B
            for(Elem elem: values) { 
                Elem tmp = ReflectionUtils.newInstance(Elem.class, conf);
			    ReflectionUtils.copy(conf, elem, tmp);
 
                if(tmp.tag ==0) {A.add(tmp);}
                else if(tmp.tag ==1) {B.add(tmp);}
                
            }  
            for(int i =0; i <A.size(); i++){
                for(int j =0; j <B.size(); j++) {
                    Pair pair =new Pair(A.get(i).index, B.get(j).index); 
                    double product = A.get(i).value *B.get(j).value; 
                    context.write(pair, new DoubleWritable(product)); 
                } 
            }
        }
    } 
    public static class MatrixMNMapper extends Mapper<Object, Text, Pair, DoubleWritable> {
		@Override
		public void map(Object key, Text line, Context context)  throws IOException, InterruptedException {  
			Scanner reader =new Scanner(line.toString());
            reader.useDelimiter("\t"); 
            String pair = reader.next();
            String[] i_j_pair =pair.split(" "); 
            double val =reader.nextDouble();  
			context.write(new Pair(Integer.parseInt(i_j_pair[0]), Integer.parseInt(i_j_pair[1])), new DoubleWritable(val));
		}
	}
	
	public static class MatrixMNReducer extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {
		@Override
		public void reduce(Pair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			for(DoubleWritable value : values)  {
                sum += value.get(); 
            }; 
            context.write(key, new DoubleWritable(sum));
		}
	}

    public int run(String[] args) throws Exception {
        Job arrangeMatrixJob = Job.getInstance();
		arrangeMatrixJob.setJobName("arrange-matrix-job");
		arrangeMatrixJob.setJarByClass(Multiply.class);

		MultipleInputs.addInputPath(arrangeMatrixJob, new Path(args[0]), TextInputFormat.class, MatrixMMapper.class);
		MultipleInputs.addInputPath(arrangeMatrixJob, new Path(args[1]), TextInputFormat.class, MatrixNMapper.class);
		arrangeMatrixJob.setReducerClass(MatrixMxMatrixNReducer.class);
		
		arrangeMatrixJob.setMapOutputKeyClass(IntWritable.class);
		arrangeMatrixJob.setMapOutputValueClass(Elem.class);
		
		arrangeMatrixJob.setOutputKeyClass(Pair.class);
		arrangeMatrixJob.setOutputValueClass(DoubleWritable.class);
		
		arrangeMatrixJob.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(arrangeMatrixJob, new Path(args[2])); 
		arrangeMatrixJob.waitForCompletion(true); 

        Job multiplyMatrixJob = Job.getInstance();
		multiplyMatrixJob.setJobName("multiply-matrix-job");
		multiplyMatrixJob.setJarByClass(Multiply.class);
		
		multiplyMatrixJob.setMapperClass(MatrixMNMapper.class);
		multiplyMatrixJob.setReducerClass(MatrixMNReducer.class);
		
		multiplyMatrixJob.setMapOutputKeyClass(Pair.class);
		multiplyMatrixJob.setMapOutputValueClass(DoubleWritable.class);
		
		multiplyMatrixJob.setOutputKeyClass(Pair.class);
		multiplyMatrixJob.setOutputValueClass(DoubleWritable.class);
		
		multiplyMatrixJob.setInputFormatClass(TextInputFormat.class);
		multiplyMatrixJob.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(multiplyMatrixJob, new Path(args[2]));
		FileOutputFormat.setOutputPath(multiplyMatrixJob, new Path(args[3]));
		
		multiplyMatrixJob.waitForCompletion(true);
        return 0; 
    }

    public static void main ( String[] args ) throws Exception {  
        ToolRunner.run(new Configuration(), new Multiply(), args);
    }
}
