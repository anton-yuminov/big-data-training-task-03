import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IpStats extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(IpStats.class);

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("mapred.output.compress", "true");
        conf.setClass("mapred.output.compression.codec", SnappyCodec.class, CompressionCodec.class);

        Job job = Job.getInstance(conf);
        job.setJobName("IP info");
        job.setJarByClass(IpStats.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IpOutInfo.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IpStats.IpInfo.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map<S, T extends BinaryComparable> extends Mapper<LongWritable, Text, Text, IpStats.IpInfo> {
        private Pattern bytesPattern = Pattern.compile(".*\\d{3}\\s(\\d+).*");
        Pattern userAgentPattern = Pattern.compile("(.*?(?:/|\\\\)\\d+\\.?\\d*).*");
        Pattern userAgentPatternSimple = Pattern.compile("((?:\\w| |-|_|\\.|!)*).*");

        protected IpInfo extractIpInfo(String line) {
            long bytesCount = 0;
            Matcher m = bytesPattern.matcher(line);
            if (m.matches()) {
                String s = m.group(1);
                bytesCount = Long.parseLong(s);
            } else {
                logger.warn("Bytes pattern is not found in string: {}", line);
            }
            IpInfo result = new IpInfo(bytesCount, 1);
            return result;
        }

        protected Text extractIpAddress(String line) {
            String ipAddress = line.substring(0, line.indexOf(" "));
            return new Text(ipAddress);
        }

        protected String extractBrowserInfo(String line) {
            // Not perfect, but ok for simplicity
            String result;

            String[] lineParts = line.split("\"");
            if (lineParts.length == 0) {
                return "";
            }
            String userAgentString = lineParts[lineParts.length - 1];
            Matcher m = userAgentPattern.matcher(userAgentString);
            if (m.matches()) {
                result = m.group(1);
            } else {
                m = userAgentPatternSimple.matcher(userAgentString);
                if (m.matches()) {
                    result = m.group(1);
                } else {
                    result = userAgentString;
                }
            }
            result = result.trim();
            if (result.length() < 2) {
                result = userAgentString;
            }
            return result.trim();
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            IpInfo iI = extractIpInfo(line);
            Text ipAddress = extractIpAddress(line);
            String userBrowser = extractBrowserInfo(line);
            context.getCounter("UserWebBrowserInfo", userBrowser).increment(1L);
            context.write(ipAddress, iI);
        }
    }

    public static class Combine extends Reducer<Text, IpStats.IpInfo, Text, IpStats.IpInfo> {

        public void reduce(Text key, Iterable<IpStats.IpInfo> values, Context context)
                throws IOException, InterruptedException {
            long amount = 0;
            int count = 0;
            for (IpStats.IpInfo ipInfo : values) {
                count += ipInfo.requestsCount.get();
                amount += ipInfo.totalBytes.get();
            }
            IpStats.IpInfo result = new IpStats.IpInfo(amount, count);
            context.write(key, result);
        }
    }

    public static class Reduce extends Reducer<Text, IpStats.IpInfo, Text, IpStats.IpOutInfo> {

        public void reduce(Text key, Iterable<IpStats.IpInfo> values, Context context)
                throws IOException, InterruptedException {
            long amount = 0;
            int count = 0;
            for (IpStats.IpInfo ipInfo : values) {
                count += ipInfo.requestsCount.get();
                amount += ipInfo.totalBytes.get();
            }
            IpStats.IpOutInfo result = new IpStats.IpOutInfo(amount, amount / count);
            context.write(key, result);
        }
    }

    public static class IpInfo implements WritableComparable<IpInfo> {

        public LongWritable totalBytes;
        public IntWritable requestsCount;

        public IpInfo(long totalBytes, int requestsCount) {
            this.totalBytes = new LongWritable(totalBytes);
            this.requestsCount = new IntWritable(requestsCount);
        }

        public IpInfo() {
            totalBytes = new LongWritable();
            requestsCount = new IntWritable();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            totalBytes.write(out);
            requestsCount.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            totalBytes.readFields(in);
            requestsCount.readFields(in);
        }

        @Override
        public int compareTo(IpInfo o) {
            int result = (int) (this.requestsCount.get() - o.requestsCount.get());
            if (result == 0) {
                result = (int) (this.totalBytes.get() - o.totalBytes.get());
            }
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IpInfo ipInfo = (IpInfo) o;

            if (totalBytes != null ? !totalBytes.equals(ipInfo.totalBytes) : ipInfo.totalBytes != null) return false;
            return requestsCount != null ? requestsCount.equals(ipInfo.requestsCount) : ipInfo.requestsCount == null;

        }

        @Override
        public int hashCode() {
            int result = totalBytes != null ? totalBytes.hashCode() : 0;
            result = 31 * result + (requestsCount != null ? requestsCount.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "IpInfo{" +
                    "totalBytes=" + totalBytes +
                    ", requestsCount=" + requestsCount +
                    '}';
        }
    }

    public static class IpOutInfo implements WritableComparable<IpOutInfo> {

        public LongWritable totalBytes;
        public FloatWritable averageBytes;

        public IpOutInfo() {
            totalBytes = new LongWritable();
            averageBytes = new FloatWritable();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IpOutInfo ipOutInfo = (IpOutInfo) o;

            if (totalBytes != null ? !totalBytes.equals(ipOutInfo.totalBytes) : ipOutInfo.totalBytes != null)
                return false;
            return averageBytes != null ? averageBytes.equals(ipOutInfo.averageBytes) : ipOutInfo.averageBytes == null;

        }

        @Override
        public int hashCode() {
            int result = totalBytes != null ? totalBytes.hashCode() : 0;
            result = 31 * result + (averageBytes != null ? averageBytes.hashCode() : 0);
            return result;
        }

        public IpOutInfo(long totalBytes, float averageBytes) {
            this.totalBytes = new LongWritable(totalBytes);
            this.averageBytes = new FloatWritable(averageBytes);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            totalBytes.write(out);
            averageBytes.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            totalBytes.readFields(in);
            averageBytes.readFields(in);
        }

        @Override
        public int compareTo(IpOutInfo o) {
            int result = (int) (this.averageBytes.get() - o.averageBytes.get());
            if (result == 0) {
                result = (int) (this.totalBytes.get() - o.totalBytes.get());
            }
            return result;
        }

        @Override
        public String toString() {
            return averageBytes.get() + "," + totalBytes.get();
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new IpStats(), args);
        System.exit(res);
    }

}