


import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class InputFormat extends TextVertexInputFormat<LongWritable, Text, LongWritable>{

    Pattern pipeSpliter = Pattern.compile(":");
    Pattern commonSpliter = Pattern.compile(",");

    @Override
    public TextVertexReader createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new InputFormatTextReader();
    }

    public class InputFormatTextReader extends TextVertexReader {
        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }
        @Override
        public Vertex<LongWritable, Text, LongWritable> getCurrentVertex() throws IOException,
                InterruptedException {
            Text line = getRecordReader().getCurrentValue();
            String[] majorParts = pipeSpliter.split(line.toString());
            LongWritable id = new LongWritable(Long.parseLong(majorParts[0]));
            Text value = new Text(majorParts[1]);

            ArrayList<Edge<LongWritable, LongWritable>> edgeIdList = new ArrayList<Edge<LongWritable, LongWritable>>();

            if (majorParts.length > 1) {
                String[] edgeIds = commonSpliter.split(majorParts[1]);
                for (String edgeId:  edgeIds) {
                    DefaultEdge<LongWritable, LongWritable> edge = new DefaultEdge<LongWritable, LongWritable>();
                    edge.setTargetVertexId(id);
                    edge.setValue(new LongWritable(Long.parseLong(edgeId)));

                    edgeIdList.add(edge);
                }
            }

            Vertex<LongWritable, Text, LongWritable> vertex = getConf().createVertex();


            vertex.initialize(id, value, edgeIdList);
            return vertex;
        }
    }
}
