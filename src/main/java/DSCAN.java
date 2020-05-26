

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


    public class DSCAN extends BasicComputation<LongWritable, Text, LongWritable, Text> {
        public Set<Integer> getNieghbors(String s){
            Set<Integer> list=new HashSet<>();
            String parts[]=s.split(",");
            for(String st: parts){
                list.add(new Integer(st));
            }
            return list;
        }

        public double getSim(String s1,String s2){
            double sim=0;
            Set<Integer> lst1=getNieghbors(s1);
            Set<Integer> lst2=getNieghbors(s2);
            Set<Integer> commun = lst1.stream().distinct().filter(lst2::contains).collect(Collectors.toSet());
            if(lst1.size()>0 || lst2.size()>0){
                sim=commun.size()/(double)Math.sqrt(lst1.size()*lst2.size());
            }
            return sim;
        }

        @Override
        public void compute(Vertex<LongWritable, Text, LongWritable> vertex, Iterable<Text> messages) throws IOException {
             if(getSuperstep()==0){
                 Text newMessage = new Text();

                 newMessage.set(vertex.getValue().toString()+":"+vertex.getId());

                // long messageSentCounter = 0;
                 for(Edge<LongWritable, LongWritable> edge: vertex.getEdges()) {
                     this.sendMessage(edge.getValue(), newMessage);
                 }


             }//cores detection
             else if(getSuperstep()==1){
                 double sim=0;
                 int nb=0;
                 for(Text m:messages){
              //    System.out.println("Vertex ID "+vertex.getId()+" and value "+vertex.getValue()+" message "+m+" from ");
                   String parts[] =m.toString().split(":");
                   if(Integer.parseInt(vertex.getId().toString())!=Integer.parseInt(parts[1])) {
                       sim = getSim(parts[0], vertex.getValue().toString());
                       if (sim >= 0.7) {
                           nb++;
                       }
                   }
                 }
                 if(nb>=3){
                     Text msg=new Text(vertex.getValue().toString()+":c");
                     vertex.setValue(msg);
                     Text newMessage = new Text();
                     newMessage.set(vertex.getValue().toString()+":"+vertex.getId());
                     for(Edge<LongWritable, LongWritable> edge: vertex.getEdges()) {
                         this.sendMessage(edge.getValue(), newMessage);
                     }
                 }
             }
             // clusrters building
             else if(getSuperstep()==2){

                 double sim=0;
                 int nb=0;
                 StringBuilder border=new StringBuilder();
                 Set<Integer> borders=new HashSet<>();
                 Set<LongWritable> outliers=new HashSet<>();
                 for(Text m:messages){
                    // System.out.println("Vertex ID "+vertex.getId()+" and value "+vertex.getValue()+" message "+m+" from ");
                     String parts[] =m.toString().split(":");
                    if(parts.length==3 ) {

                         sim = getSim(parts[0], vertex.getValue().toString().split(":")[0]);
                         if (sim >= 0.7) {
                            border.append(","+parts[2]);
                            borders.add(Integer.parseInt(parts[2]));
                         }
                     }
                 }

                 Integer max=-1;
                 for(Integer i : borders){
                     if(i>max) max=i;
                 }
                 Text msg=new Text("");
                 if(vertex.getValue().toString().split(":").length==1 & max>-1 ) {
                     //borders
                     msg = new Text(vertex.getId()+":"+vertex.getValue().toString() + ":" + max);
                 }else if (vertex.getValue().toString().split(":").length==2)
                 {
                     //cores
                     msg = new Text(vertex.getId()+":"+vertex.getValue().toString().split(":")[0] + ":" + max);
                 }
                 else{
                     // outliers or bridges
                     msg = new Text(vertex.getId()+":"+vertex.getValue().toString());
                 }
                 vertex.setValue(msg);

                 for(Edge<LongWritable, LongWritable> edge: vertex.getEdges()) {
                    // if(msg.toString().split(":").length==1){
                     this.sendMessage(edge.getValue(), msg);
                    // }
                 }

             }
             // outliers and bridges detection
             else if(getSuperstep()==3){
                 Set<Integer> neig=new HashSet<>();
                 for(Text m:messages){

                    if(vertex.getValue().toString().split(":").length==2 & Integer.parseInt(vertex.getId().toString().split(":")[0])!=Integer.parseInt(m.toString().split(":")[0])){
                        if(m.toString().split(":").length==3){
                           neig.add(Integer.parseInt(m.toString().split(":")[2]));
                       }
                    }
                 }
                 Text msg=new Text("");

                 if((neig.size()==0 || neig.size()==1 ) & vertex.getValue().toString().split(":").length==2){
                     msg.set(vertex.getId().toString()+":-2");
                 }else if (neig.size()>=2 & vertex.getValue().toString().split(":").length==2){
                     msg.set(vertex.getId().toString()+":-1");
                 }
                 if (vertex.getValue().toString().split(":").length==3){
                     msg.set(vertex.getValue().toString().split(":")[0]+":"+vertex.getValue().toString().split(":")[2]);
                 }
                 vertex.setValue(msg);


                 vertex.voteToHalt();
             }

        }
    }

