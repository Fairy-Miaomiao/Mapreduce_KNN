package KNNCode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class IntSumReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
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
