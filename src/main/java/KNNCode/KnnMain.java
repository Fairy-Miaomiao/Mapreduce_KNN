package KNNCode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class KnnMain {


    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private final static IntWritable one = new IntWritable(1);
        static Object[] values = new Integer[45];
        private Text word = new Text();
        static List<String> test = new ArrayList<String>(); //存储test测试集
        static List<String> trains = new ArrayList<String>();
        static int num = 0;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException { //获取缓存文件路径的数组
            Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            //System.out.println(paths[0]);
            test.clear();
            String input = "E:\\IdeaProjects\\homework07\\input\\test\\iris_test.csv";
            BufferedReader sb = new BufferedReader(new FileReader(input));
            //读取BufferedReader里面的数据
            /*String tmp = null;
            while ((tmp = sb.readLine()) != null) {
                String id = tmp.split(",")[0].replaceAll("\"", "");
                for (int i = 0; i < values.length; i++) {
                    if (Integer.parseInt(id) == Integer.parseInt(values[i].toString()))
                        test.add(tmp);
                    else
                        trains.add(tmp);
                }
            }*/
            String tmp = null;
            while ((tmp = sb.readLine()) != null) {
                test.add(tmp);
            }
            System.out.println(test.size() + "***********");
            //关闭sb对象
            sb.close();
            //System.out.println("+++++++" + test);
        }

        //计算欧式距离
        private double Distance(Double[] a, Double[] b) {
            // TODO Auto-generated method stub
            double sum = 0.0;
            for (int i = 0; i < a.length; i++) {
                sum += Math.pow(a[i] - b[i], 2);
            }
            return Math.sqrt(sum);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {
            String train[] = value.toString().split(",");
            //System.out.println(train[0]);
            //System.out.println(test.size());
            String lable = train[train.length - 1];
            //System.out.println(lable);
            //训练集由字符格式转化为Double数组
            String id = train[0].split(",")[0].replaceAll("\"", "");
            for (int j = 0; j < values.length; j++) {
                Double[] train_point = new Double[5];
                int n = 0;
                for (String it : train) {
                    //System.out.println(i);
                    if (n >= 5)
                        break;
                    train_point[n] = Double.valueOf(it.replaceAll("\"", ""));
                    n = n + 1;
                }
                //测试集由字符格式转化为Double数组
                for (int i = 0; i < test.size(); i++) {
                    String test_poit1[] = test.get(i).toString().split(",");
                    Double[] test_poit = new Double[5];
                    int q = 0;
                    for (String it : test_poit1) {
                        if (q >= 5)
                            break;
                        test_poit[q] = Double.valueOf(it.replaceAll("\"", ""));
                        q = q + 1;
                    }
                    //每个测试点的ID作为键key，计算每个测试点与该训练点的距离+"@"+类标签   作为value
                    //System.out.println(Distance(test_poit, train_point));
                    context.write(new IntWritable(i), new Text(String.valueOf(Distance(test_poit, train_point)) + "@" + lable));
                }
                break;

            }
        }
    }

    public static class InvertedIndexCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
        private Text info = new Text();
        int k;

        protected void setup(Context context) throws IOException,
                InterruptedException {
            Configuration conf = context.getConfiguration();
            k = conf.getInt("K", 1);
        }

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException,
                InterruptedException {
            //排序
            TreeMap<Double, String> treemap = new TreeMap<Double, String>();
            int sum = 0;
            for (Text val : values) {
                String distance_lable[] = val.toString().split("@");
                for (int i = 0; i < distance_lable.length - 1; i = i + 2) {
                    treemap.put(Double.valueOf(distance_lable[i]), distance_lable[i + 1]);
                    //treemap会自动按key升序排序，也就是距离小的排前面
                }
            }
            //得到前k项距离最近
            Iterator<Double> it = treemap.keySet().iterator();
            Map<String, Integer> map = new HashMap<String, Integer>();
            int num = 0;
            String valueinfo = "";
            while (it.hasNext()) {
                Double key1 = it.next();
                valueinfo += String.valueOf(key1) + "@" + treemap.get(key1) + "@";
                num++;
                if (num > k)
                    break;
            }
            context.write(key, new Text(valueinfo));

        }
    }


    public static class IntSumReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {
        private Text result = new Text();
        int k;
        static int sum = 0;
        static int rightCount = 0;
        double ratio = 0.3;
        int total = 150;

        protected void setup(Context context) throws IOException,
                InterruptedException {
            Configuration conf = context.getConfiguration();
            k = conf.getInt("K", 1);
        }

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException,
                InterruptedException {
            //排序
            TreeMap<Double, String> treemap = new TreeMap<Double, String>();
            for (Text val : values) {
                String distance_lable[] = val.toString().split("@");
                for (int i = 0; i < distance_lable.length - 1; i = i + 2) {
                    treemap.put(Double.valueOf(distance_lable[i]), distance_lable[i + 1]);
                    //treemap会自动按key升序排序，也就是距离小的排前面
                }
            }
            //得到前k项距离最近
            Iterator<Double> it = treemap.keySet().iterator();
            Map<String, Integer> map = new HashMap<String, Integer>();
            int num = 0;
            while (it.hasNext()) {
                Double key1 = it.next();
                if (map.containsKey(treemap.get(key1))) {
                    int temp = map.get(treemap.get(key1));
                    map.put(treemap.get(key1), temp + 1);
                } else {
                    map.put(treemap.get(key1), 1);
                }
                //System.out.println(key1+"="+treemap.get(key1));
                num++;
                if (num > k)
                    break;
            }
            //得到排名最靠前的标签为test的类别
            Iterator<String> it1 = map.keySet().iterator();
            String lable = it1.next();
            int count = map.get(lable);
            while (it1.hasNext()) {
                String now = it1.next();
                if (count < map.get(now)) {
                    lable = now;
                    count = map.get(lable);
                }
            }

            result.set(lable);
            Path test = new Path("E:\\IdeaProjects\\homework07\\input\\test\\iris_test.csv");
            BufferedReader sb1 = new BufferedReader(new FileReader(test.toUri().getPath()));
            String tmpre = null;
            int countnum = 0;
            tmpre = sb1.readLine();
            while (countnum < Integer.parseInt(key.toString())) {
                tmpre = sb1.readLine();
                ++countnum;
            }
            String tes[] = tmpre.toString().split(",");
            if (tes[tes.length - 1].equals(lable))
                ++rightCount;
            ++sum;
            context.write(key, result);


        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Double acu=100.0*rightCount/sum;
            String acuInput=acu.toString();
            context.write(null,new Text("Accuracy is:"+acuInput+"%"));
        }
    }

    static FileSystem fs;

    public static void main(String[] args) throws Exception {
        //String inputFile = "E:\\IdeaProjects\\homework07\\input\\train";
        String inputFile = args[0]+"train";
        //任务一
        Configuration conf = new Configuration();
        String[] otherArgs = new String[]{
                args[0]+"train",
                args[1]
        };
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        System.out.println(args[0]);
        conf.setInt("K", 5);
        Job job1 = Job.getInstance(conf, "word count");
        job1.setJarByClass(WordCount.class);
        //设置分布式缓存文件
        //job1.addCacheFile(new URI("hdfs://localhost:9000/input/iris/test/iris_test_data.csv"))
        //job1.addCacheFile(new URI("file:///E:/IdeaProjects/homework07/input/test"));
        job1.addCacheFile(new URI(args[0]+"test"));
        fs = FileSystem.get(new Configuration());
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(InvertedIndexCombiner.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        //Path outputFile = new Path("E:\\IdeaProjects\\homework07\\output");
        Path outputFile=new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputFile)) {
            fs.delete(outputFile, true);
        }
        FileInputFormat.setInputPaths(job1, inputFile);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job1,
                new Path(otherArgs[otherArgs.length - 1]));

        job1.waitForCompletion(true);

    }


}




