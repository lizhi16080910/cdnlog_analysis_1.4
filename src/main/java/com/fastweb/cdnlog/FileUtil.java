/** 
 * @Title: FileUtil.java 
 * @Package com.fastweb.cdnlog.bandcheck.util 
 * @author LiFuqiang 
 * @date 2016年7月13日 下午3:08:55 
 * @version V1.0 
 * @Description: TODO(用一句话描述该文件做什么) 
 * Update Logs: 
 * **************************************************** * Name: * Date: * Description: ****************************************************** 
 */
package com.fastweb.cdnlog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: FileUtil
 * @author LiFuqiang
 * @date 2016年7月13日 下午3:08:55
 * @Description: TODO(这里用一句话描述这个类的作用)
 */
public class FileUtil {



    public static void listToFile(String path, List<String> list, String charSet) {
        File file = new File(path);
        File parentDir = file.getParentFile();
        if (!parentDir.exists()) {
            parentDir.mkdirs();
        }
        Writer writer = null;
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(path);
            writer = new OutputStreamWriter(fos, charSet);

            for (String str : list) {
                writer.write(str);
                writer.write("\n");
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (writer != null) {
                    writer.close();
                }
                if (fos != null) {
                    fos.close();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public static List<String> readFile(String path, String charSet) {
        return readFile(new File(path), charSet);
    }

    public static List<String> readFile(File file, String charSet) {
        List<String> list = new ArrayList<>();
        // String encode = getFileEncode(path);
        // System.out.println(encode);
        FileInputStream fInputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader buffreader = null;
        try {
            fInputStream = new FileInputStream(file);
            inputStreamReader = new InputStreamReader(fInputStream, charSet);
            buffreader = new BufferedReader(inputStreamReader);
            String line = null;
            while ((line = buffreader.readLine()) != null) {
                list.add(line);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (fInputStream != null) {
                    fInputStream.close();
                }
                if (inputStreamReader != null) {
                    inputStreamReader.close();
                }
                if (buffreader != null) {
                    buffreader.close();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }

        }
        return list;
    }

    public static List<String> readFile(File file) {
        return readFile(file, "utf8");
    }

    public static List<String> readFile(String path) {
        return readFile(path, "utf8");
    }

    public static void listToFile(String path, List<String> list) {
        listToFile(path, list, "utf8");
    }

    public static List<String> readHdfsFile(String path) {
        List<String> list = new ArrayList<>();
        FileSystem fs = null;
        FSDataInputStream inputStream = null;
        BufferedReader reader = null;
        Configuration conf = new Configuration();
        try {
            fs = FileSystem.get(conf);
            inputStream = fs.open(new Path(path));
            reader = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while ((line = reader.readLine()) != null) {
                list.add(line);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (reader != null) {
                    reader.close();
                }
                if (inputStream != null) {
                    inputStream.close();
                }
                if (fs != null) {
                    fs.close();
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    public static List<String> readDomains(String path) throws IOException {
        List<String> domains = new ArrayList<>();
        FileReader fileReader = new FileReader(new File(path));
        BufferedReader bufReader = new BufferedReader(fileReader);
        String line = null;
        while ((line = bufReader.readLine()) != null) {
            domains.add(line.trim());
        }
        bufReader.close();
        fileReader.close();
        return domains;
    }

    public static void deleteHdfsFile(String path) throws IOException {
        deleteHdfsFile(new Path(path));
    }

    public static void deleteHdfsFile(Path path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(!fs.exists(path)){
            return;
        }
        fs.delete(path,true);
        fs.close();
    }

    public static void moveHdfsFile(String src,String dst) throws IOException {
        moveHdfsFile(new Path(src),new Path(dst));
    }

    public static void moveHdfsFile(Path src,Path dst) throws IOException {
        Configuration conf = new Configuration();
        //conf.set("fs.default.name", "hdfs://192.168.100.202:8020");
        FileSystem fs = FileSystem.get(conf);
        fs.rename(src,dst);
        fs.close();
    }

    /**
     *
     * @param srcpath，源目录
     * @param dstpath，目的目录
     * @param date, 时间标记
     * @throws IOException
     * 移动一个目录下的所有文件（包括子目录的文件）到另外一个目录下
     */
    public static void moveLogFile(String srcpath,String dstpath,String date) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path dir = new Path(srcpath);
        RemoteIterator<LocatedFileStatus> domains = fs.listFiles(dir, true);
        while (domains.hasNext()) {
            Path domainPath = domains.next().getPath();
            String domainPathString = domainPath.toString();
            if (domainPathString.contains(date)) {
                String dstStr = domainPathString.replace(srcpath, dstpath);
                Path dstPath = new Path(dstStr);
                fs.mkdirs(dstPath.getParent());
                fs.rename(domainPath, dstPath);
            }
        }
        fs.delete(dir, true);
        fs.close();
    }

    public static void createHdfsDir(Path path) throws IOException {
        Configuration conf = new Configuration();
        //conf.set("fs.default.name", "hdfs://192.168.100.202:8020");
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(path);
        fs.close();
    }

    public static void createHdfsDir(String path) throws IOException {
        createHdfsDir(new Path(path));
    }

    /**
     * @Title: main
     * @param args
     * @throws
     * @Description: TODO(这里用一句话描述这个方法的作用)
     */
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub


    }
}
