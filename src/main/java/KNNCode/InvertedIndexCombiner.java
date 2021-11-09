package KNNCode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class InvertedIndexCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
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
