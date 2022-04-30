import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

  private static final String DELIMITER = "#";

  public static class JoinMapper
      extends Mapper<Object, Text, Text, Text> {

    private static final int YEAR_COL = 53;
    private static final int SONG_HOTNESS_COL = 48;
    private static final int TEMPO_COL = 28;
    private static final int LOUDNESS_COL = 24;
    private static final int DURATION_COL = 4;

//    @Override
//    public void setup(Context context) {
//
//    }

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      CSVParser parser = CSVParser.parse(value.toString(), CSVFormat.DEFAULT);
      String[] values = parser.getRecords().get(0).toList().toArray(new String[0]);

      int year = Integer.parseInt(values[YEAR_COL]);
      if (year != 0 && !values[SONG_HOTNESS_COL].equals("")) {
        Text reducerKey = new Text();
        reducerKey.set(String.valueOf(year));
        Text reducerValue = new Text();
        String stringValue = values[SONG_HOTNESS_COL] + DELIMITER
            + values[TEMPO_COL] + DELIMITER
            + values[LOUDNESS_COL] + DELIMITER
            + values[DURATION_COL];
        reducerValue.set(stringValue);
        context.write(reducerKey, reducerValue);
      }
    }

//    @Override
//    public void cleanup(Context context)
//        throws IOException, InterruptedException {
//      super.cleanup(context);
//    }
  }

  public static class JoinReducer
      extends Reducer<Text, Text, Text, Text> {

    Map<String, PriorityQueue<double[]>> map;

    @Override
    public void setup(Context context) {
      map = new HashMap<>();
//      pq = new PriorityQueue<>();
    }

    public void reduce(Text key, Iterable<Text> values, Context context) {
      PriorityQueue<double[]> pq = new PriorityQueue<>((a, b) -> Double.compare(a[0], b[0]));
      if (map.containsKey(key.toString())) {
        pq = map.get(key.toString());
      } else {
        map.put(key.toString(), pq);
      }
      for (Text val : values) {
        double[] temp = Arrays.stream(val.toString().split(DELIMITER))
            .mapToDouble(Double::parseDouble).toArray();

        pq.add(temp);
        if (pq.size() > 100) {
          pq.poll();
        }
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      double averageTempo = 0;
      double averageLoudness = 0;
      double averageDuration = 0;

      for (Entry<String, PriorityQueue<double[]>> entry : map.entrySet()) {
        String key = entry.getKey();
        PriorityQueue<double[]> pq = entry.getValue();

        for (double[] data : pq) {
          if (data != null) {
            averageTempo += data[1];
            averageLoudness += data[2];
            averageDuration += data[3];
          }
        }

        int size = pq.size();
        averageTempo /= size;
        averageLoudness /= size;
        averageDuration /= size;

        String reduceValue = String.format("%.2f, %.2f, %.2f %d", averageTempo, averageLoudness,
            averageDuration, size);
        context.write(new Text(key), new Text(reduceValue));
      }

    }

  }

  public static void main(String[] args) throws Exception {
    // Job 1 configuration
    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "Music-Analysis");
    job1.setJarByClass(Driver.class);
    job1.setMapperClass(JoinMapper.class);
    job1.setReducerClass(JoinReducer.class);

    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));

    System.exit(job1.waitForCompletion(true) ? 0 : 1);
  }
}