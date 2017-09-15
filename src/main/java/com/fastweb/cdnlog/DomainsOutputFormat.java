/**
 * @Title: MultipleOutputFormat.java
 * @Package lifq.multiple.output
 * @author LiFuqiang
 * @date 2016年5月19日 下午4:31:34
 * @version V1.0
 * @Description: TODO(用一句话描述该文件做什么)
 * Update Logs:
 * **************************************************** * Name: * Date: * Description: ******************************************************
 */
package com.fastweb.cdnlog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * @ClassName: MultipleOutputFormat
 * @author LiFuqiang
 * @date 2016年5月19日 下午4:31:34
 * @Description: TODO(这里用一句话描述这个类的作用)
 */
public class DomainsOutputFormat extends FileOutputFormat<Text, Text> {
    private RecordWriter<Text, Text> writer = null;

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException,
            InterruptedException {
        if (writer == null) {
            writer = new MultiRecordWriter(job, getTaskOutputPath(job));
        }
        return writer;
    }

    private Path getTaskOutputPath(TaskAttemptContext conf) throws IOException {
        Path workPath = null;
        OutputCommitter committer = super.getOutputCommitter(conf);
        if (committer instanceof FileOutputCommitter) {
            workPath = ((FileOutputCommitter) committer).getWorkPath();
        } else {
            Path outputPath = super.getOutputPath(conf);
            if (outputPath == null) {
                throw new IOException("Undefined job output-path");
            }
            workPath = outputPath;
        }
        System.out.println(workPath);
        return workPath;
    }

    protected String generateFileNameForKeyValue(Text key, Text value, Configuration conf) {
        //return key.toString() + Path.SEPARATOR + "2016/07" + ".gz";
        //String date = conf.get("work.date");
        return key.toString() + ".gz";
    }

    public class MultiRecordWriter extends RecordWriter<Text, Text> {
        /** RecordWriter的缓存,K表示文件的完整路径 */
        private HashMap<String, RecordWriter<Text, Text>> recordWriters = null;

        private TaskAttemptContext job = null;
        /** 输出目录 */
        private Path workPath = null;

        public MultiRecordWriter(TaskAttemptContext job, Path workPath) {
            super();
            this.job = job;
            this.workPath = workPath;
            recordWriters = new HashMap<String, RecordWriter<Text, Text>>();
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            Iterator<RecordWriter<Text, Text>> values = this.recordWriters.values().iterator();
            while (values.hasNext()) {
                values.next().close(context);
            }
            this.recordWriters.clear();
        }

        @Override
        public void write(Text key, Text value) throws IOException, InterruptedException {
            // 得到输出文件名
            String baseName = generateFileNameForKeyValue(key, value, job.getConfiguration());
            RecordWriter<Text, Text> rw = this.recordWriters.get(baseName);
            if (rw == null) {
                try {
                    rw = getBaseRecordWriter(job, baseName);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                this.recordWriters.put(baseName, rw);
            }
            rw.write(key, value);
        }

        // ${mapred.out.dir}/_temporary/_${taskid}/${nameWithExtension}
        private RecordWriter<Text, Text> getBaseRecordWriter(TaskAttemptContext job, String baseName)
                throws IOException, InterruptedException, ClassNotFoundException {
            Configuration conf = job.getConfiguration();

            String keyValueSeparator = "#_#";
            job.getTaskAttemptID().getTaskID().toString();
            Path file = new Path(workPath + Path.SEPARATOR + baseName);
            Class.forName("org.apache.hadoop.io.compress.GzipCodec");
//            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job,
//                    GzipCodec.class);
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(GzipCodec.class, conf);
            //Path file = new Path(workPath, baseName + codec.getDefaultExtension());
            FSDataOutputStream fileOut = file.getFileSystem(conf).create(file, false);
            RecordWriter<Text, Text> recordWriter = new LineRecordWriter<Text, Text>(new DataOutputStream(
                    codec.createOutputStream(fileOut)), keyValueSeparator);
            return recordWriter;
        }
    }

}
