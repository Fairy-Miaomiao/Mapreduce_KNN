package KNNCode;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text> {

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
