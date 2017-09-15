package com.fastweb.cdnlog;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DomainReporter {
    private static final Log LOG = LogFactory.getLog(DomainReporter.class);
    private static final int tryNumber = 3;

    /**
     * 向指定 URL 发送POST方法的请求
     * 
     * @param postUrl
     *            请求URL
     * @param postBody
     *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式
     * @return 所代表远程资源的响应结果
     */
    public static String postData(String postUrl, String postBody) {
        PrintWriter out = null;
        BufferedReader in = null;
        String result = "";
        try {
            URL realUrl = new URL(postUrl);
            // 打开和URL之间的连接
            URLConnection conn = realUrl.openConnection();
            // 设置通用的请求属性
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 发送POST请求必须设置如下两行
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // 获取URLConnection对象对应的输出流
            out = new PrintWriter(conn.getOutputStream());
            // 发送请求参数
            out.print(postBody);
            // flush输出流的缓冲
            out.flush();
            // 定义BufferedReader输入流来读取URL的响应
            in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        }
        catch (Exception ex) {
            System.out.println("发送 POST 请求出现异常！" + ex);
            //LOG.info("发送 POST 请求出现异常！" + ex);
        }
        // 使用finally块来关闭输出流、输入流
        finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (in != null) {
                    in.close();
                }
            }
            catch (IOException ex) {

            }
        }
        return result;
    }

    public static void submitDomainHourly(Map<String, String> domainConfMap,
            Set<String> needPostDomainSet, String ts, String day, String hour, String fcname,
            String fckey, String postUrl) throws FileNotFoundException, IOException,
            InterruptedException {

        String domainName;
        Set<String> hourlyPostDomainSet = new HashSet<>();
        Set<String> savedCompleteDomainSet = new HashSet<>();
        Iterator<String> iterator;
        File dataDirPath = new File("/usr/local/terasort/data/" + day);
        if (!dataDirPath.exists()) {
            dataDirPath.mkdirs();
        }
        File[] list = dataDirPath.listFiles();
        for (File file : list) {
            if (file.isFile()) {
                try (InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
                        BufferedReader br = new BufferedReader(reader)) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (!savedCompleteDomainSet.contains(line)) {
                            savedCompleteDomainSet.add(line);
                        }
                    }
                    br.close();
                    reader.close();
                }
                catch (IOException e) {
                }
            }
        }
        // 写日志(追加模式)
        try (FileWriter fw = new FileWriter(
                "/usr/local/terasort/data/" + day + "/" + hour + ".log", true)) {
            iterator = needPostDomainSet.iterator();
            int idx;
            String domainConf = null;
            while (iterator.hasNext()) {

                domainName = iterator.next();

                if (-1 != (idx = domainName.indexOf("."))
                        && (domainConf = domainConfMap.get(domainName.substring(idx + 1))) == null
                        && (domainConf = domainConfMap.get(domainName)) == null) {
                    LOG.info("domain " + domainName + " not found," + "\tsub_domain "
                            + domainName.substring(idx + 1) + " not found");
                    continue;
                }
                idx = domainConf.indexOf(" ");
                String mergeFlag = domainConf.substring(0, idx);
                String dirFlag = domainConf.substring(idx + 1);
                switch (mergeFlag) {
                // 按天合并
                case "0":
                    if (!savedCompleteDomainSet.contains(domainName)) // 这个域名没有被处理过
                    {
                        fw.write(domainName);
                        fw.write("\n");
                    }
                    break;
                // 按小时合并
                case "1":
                    if (!hourlyPostDomainSet.contains(domainName)) {
                        hourlyPostDomainSet.add(domainName + " "
                                + ((dirFlag.equals("0") == true) ? "0" : "1"));
                    }
                    break;
                default:
                    break;
                }
            }
            fw.flush();
            fw.close();
        }
        String postStr;
        String statusStr;
        String statusStrPredix = "\"status\":";
        int idx1;
        int idx2;

        String fctoken = DigestUtils.md5Hex(new SimpleDateFormat("yyyyMMdd").format(new Date(System
                .currentTimeMillis())) + DigestUtils.md5Hex(fckey));
        int currentDomainCount = 0;
        StringBuilder sb = new StringBuilder();
        iterator = hourlyPostDomainSet.iterator();
        while (iterator.hasNext()) {
            domainName = iterator.next();
            currentDomainCount++;

            if ((currentDomainCount - 1) % 300 == 0) {
                sb.append("{\"time\":\"");
                sb.append(ts);
                sb.append("\",\"domain\":[");
                sb.append("\"").append(StringEscapeUtils.escapeJson(domainName)).append("\"");
            }
            else if (currentDomainCount % 300 == 0) {
                sb.append(",").append("\"").append(StringEscapeUtils.escapeJson(domainName))
                        .append("\"");
                sb.append("],");
                sb.append("\"fcname\":").append("\"").append(fcname).append("\",");
                sb.append("\"fctoken\":").append("\"").append(fctoken).append("\"");
                sb.append("}");

                // 开始调用北京post接口
                // 替换转移字符 '\'
                postStr = sb.toString()/* .replaceAll("\\", "\\\\") */;
                LOG.info(postStr + "\n");
                for (;;) {
                    statusStr = "";
                    String result = postData(postUrl, postStr);
                    LOG.info(result);
                    idx1 = result.indexOf(statusStrPredix);
                    if (idx1 != -1) {
                        idx2 = result.indexOf(",");
                        if (idx2 != -1) {
                            statusStr = result.substring(idx1 + statusStrPredix.length(), idx2);
                            LOG.info("statusStr = " + statusStr + "\n");
                        }
                    }
                    if (statusStr.length() == 0 || statusStr.equals("0"))// failed
                    {
                        Thread.sleep(1000);
                    }
                    else {
                        break;
                    }
                }
                sb.delete(0, sb.length());
            }
            else {
                sb.append(",").append("\"").append(StringEscapeUtils.escapeJson(domainName))
                        .append("\"");
            }
        }
        if (currentDomainCount % 300 != 0) {
            sb.append("],");
            sb.append("\"fcname\":").append("\"").append(fcname).append("\",");
            sb.append("\"fctoken\":").append("\"").append(fctoken).append("\"");
            sb.append("}");

            // 开始调用北京post接口
            // 替换转移字符 '\'
            postStr = sb.toString()/* .replaceAll("\\", "\\\\") */;
            LOG.info(postStr + "\n");
            for (;;) {
                statusStr = "";
                String result = postData(postUrl, postStr);
                LOG.info(result);
                idx1 = result.indexOf(statusStrPredix);
                if (idx1 != -1) {
                    idx2 = result.indexOf(",");
                    if (idx2 != -1) {
                        statusStr = result.substring(idx1 + statusStrPredix.length(), idx2);
                        LOG.info("statusStr = " + statusStr + "\n");
                    }
                }
                if (statusStr.length() == 0 || statusStr.equals("0"))// failed
                {
                    Thread.sleep(1000);
                }
                else {
                    break;
                }
            }
        }
    }

    public static void submitDomainDaily(Map<String, String> domainConfMap, String ts, String day,
            String fcname, String fckey, String posturl) throws FileNotFoundException, IOException,
            InterruptedException {

        String domainName;
        Set<String> dailyPostDomainSet = new HashSet<>();
        Set<String> savedCompleteDomainSet = new HashSet<>();
        Iterator<String> iterator;
        File dataDirPath = new File("/usr/local/terasort/data/" + day);
        if (!dataDirPath.exists()) {
            dataDirPath.mkdirs();
        }
        File[] list = dataDirPath.listFiles();
        for (File file : list) {
            if (file.isFile()) {
                try (InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
                        BufferedReader br = new BufferedReader(reader)) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (!savedCompleteDomainSet.contains(line)) {
                            savedCompleteDomainSet.add(line);
                        }
                    }
                    br.close();
                    reader.close();
                }
                catch (IOException e) {
                }
            }
        }

        String domainConf = null;
        String mergeFlag;
        String dirFlag;
        int idx;
        iterator = savedCompleteDomainSet.iterator();
        while (iterator.hasNext()) {
            domainName = iterator.next();
            if (-1 != (idx = domainName.indexOf("."))
                    && (domainConf = domainConfMap.get(domainName.substring(idx + 1))) == null
                    && (domainConf = domainConfMap.get(domainName)) == null) {
                LOG.info("domain " + domainName + " not found," + "\tsub_domain "
                        + domainName.substring(idx + 1) + " not found");
                continue;
            }
            idx = domainConf.indexOf(" ");
            mergeFlag = domainConf.substring(0, idx);
            dirFlag = domainConf.substring(idx + 1);
            switch (mergeFlag) {
            // 按天合并
            case "0":
                if (!dailyPostDomainSet.contains(domainName)) {
                    dailyPostDomainSet.add(domainName + " "
                            + ((dirFlag.equals("0") == true) ? "0" : "1"));
                }
                break;
            default:
                break;
            }
        }

        String postStr;
        String statusStr;
        String statusStrPredix = "\"status\":";
        int idx1;
        int idx2;

        String fctoken = DigestUtils.md5Hex(new SimpleDateFormat("yyyyMMdd").format(new Date(System
                .currentTimeMillis())) + DigestUtils.md5Hex(fckey));
        int currentDomainCount = 0;
        StringBuilder sb = new StringBuilder();
        iterator = dailyPostDomainSet.iterator();
        while (iterator.hasNext()) {
            domainName = iterator.next();
            currentDomainCount++;

            if ((currentDomainCount - 1) % 300 == 0) {
                sb.append("{\"time\":\"");
                sb.append(ts);
                sb.append("\",\"domain\":[");
                sb.append("\"").append(StringEscapeUtils.escapeJson(domainName)).append("\"");
            }
            else if (currentDomainCount % 300 == 0) {
                sb.append(",").append("\"").append(StringEscapeUtils.escapeJson(domainName))
                        .append("\"");
                sb.append("],");
                sb.append("\"fcname\":").append("\"").append(fcname).append("\",");
                sb.append("\"fctoken\":").append("\"").append(fctoken).append("\"");
                sb.append("}");

                // 开始调用北京post接口
                // 替换转移字符 '\'
                postStr = sb.toString()/* .replaceAll("\\", "\\\\") */;
                LOG.info(postStr + "\n");
                for (;;) {
                    statusStr = "";
                    String result = postData(posturl, postStr);
                    LOG.info(result);
                    idx1 = result.indexOf(statusStrPredix);
                    if (idx1 != -1) {
                        idx2 = result.indexOf(",");
                        if (idx2 != -1) {
                            statusStr = result.substring(idx1 + statusStrPredix.length(), idx2);
                            LOG.info("statusStr = " + statusStr + "\n");
                        }
                    }
                    if (statusStr.length() == 0 || statusStr.equals("0"))// failed
                    {
                        Thread.sleep(1000);
                    }
                    else {
                        break;
                    }
                }
                sb.delete(0, sb.length());
            }
            else {
                sb.append(",").append("\"").append(StringEscapeUtils.escapeJson(domainName))
                        .append("\"");
            }
        }
        if (currentDomainCount % 300 != 0) {
            sb.append("],");
            sb.append("\"fcname\":").append("\"").append(fcname).append("\",");
            sb.append("\"fctoken\":").append("\"").append(fctoken).append("\"");
            sb.append("}");

            // 开始调用北京post接口
            // 替换转移字符 '\'
            postStr = sb.toString()/* .replaceAll("\\", "\\\\") */;
            LOG.info(postStr + "\n");
            for (;;) {
                statusStr = "";
                String result = postData(posturl, postStr);
                LOG.info(result);
                idx1 = result.indexOf(statusStrPredix);
                if (idx1 != -1) {
                    idx2 = result.indexOf(",");
                    if (idx2 != -1) {
                        statusStr = result.substring(idx1 + statusStrPredix.length(), idx2);
                        LOG.info("statusStr = " + statusStr + "\n");
                    }
                }
                if (statusStr.length() == 0 || statusStr.equals("0"))// failed
                {
                    Thread.sleep(1000);
                }
                else {
                    break;
                }
            }
        }
    }

    public static boolean submitDomain5minute(String logType,String logNode,List<String> domains, String ts, String fcname,
                                              String fckey, String posturl) throws IOException, InterruptedException {

        String domainName;
        //String logType = "5minute";
        String postStr;
        String statusStr;
        String statusStrPredix = "\"status\":";
        int idx1;
        int idx2;

        String fctoken = DigestUtils.md5Hex(new SimpleDateFormat("yyyyMMdd").format(new Date(System
                .currentTimeMillis())) + DigestUtils.md5Hex(fckey));

        // 拼接字符串
        StringBuilder sb = new StringBuilder();
        sb.append("{\"time\":\"");
        sb.append(ts);
        sb.append("\"," + "\"log_type\":\"");
        sb.append(logType);
        sb.append("\"," + "\"log_node\":\"");
        sb.append(logNode);
        sb.append("\",\"domain\":[");

        Iterator<String> iterator = domains.iterator();
        while (iterator.hasNext()) {
            domainName = iterator.next();
            sb.append("\"").append(StringEscapeUtils.escapeJson(domainName)).append("\"");
            sb.append(",");
        }
        sb.deleteCharAt(sb.lastIndexOf(","));
        sb.append("],");
        sb.append("\"fcname\":").append("\"").append(fcname).append("\",");
        sb.append("\"fctoken\":").append("\"").append(fctoken).append("\"");
        sb.append("}");
        postStr = sb.toString();
        //LOG.info(postStr + "\n");
        System.out.println(postStr + "\n");
        int tryNo = tryNumber;
        for (;;) {

            statusStr = "";
            String result = postData(posturl, postStr);
            //LOG.info(result);
            System.out.println(result);
            idx1 = result.indexOf(statusStrPredix);
            if (idx1 != -1) {
                idx2 = result.indexOf(",");
                if (idx2 != -1) {
                    statusStr = result.substring(idx1 + statusStrPredix.length(), idx2);
                    //LOG.info("statusStr = " + statusStr + "\n");
                    System.out.println("statusStr = " + statusStr + "\n");
                }
            }
            if (statusStr.length() == 0 || statusStr.equals("0"))// failed
            {
                tryNo--;
                if (tryNo == 0) {
                    return false;
                }
                Thread.sleep(5000);
            }
            else {
                break;
            }
        }
        return true;
    }

    public static boolean submitDomain5minute(List<String> domains, String ts, String fcname,
            String fckey, String posturl) throws IOException, InterruptedException {
        return submitDomain5minute("5minute","edge",domains,ts,fcname,fckey,posturl);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ParseException {
        if (args.length != 1) {
            System.out.println("usage:<confFile>");
            System.exit(0);
        }
        String confFile = args[0];
        Properties props = new Properties();
        FileReader reader = new FileReader(confFile);
        props.load(reader);
        reader.close();
        String pushFailedDir = props.getProperty("parent.domain.log.push.failed.path");

        String fcname = props.getProperty("cdnlog.merge.domain.post.fcname");
        String fckey = props.getProperty("cdnlog.merge.domain.post.fckey");
        String postUrl = props.getProperty("cdnlog.domain.log.postUrl");

        File dir = new File(pushFailedDir);
        File[] pushFailedMessage =dir.listFiles();
        if(pushFailedMessage.length == 0){
            System.out.println("no message to push in the push failed message directory" );
            return;
        }
        for(File file:pushFailedMessage){
            List<String> ds = FileUtil.readFile(file);
            String ts = String.valueOf(new SimpleDateFormat("yyyy-MM-dd-HH-mm").parse(file.getName())
                    .getTime() / 1000);
            boolean pushOk = submitDomain5minute("5minute","parent",ds,ts,fcname,fckey,postUrl);
            if(pushOk){
                file.delete();
            }
        }
    }

    public static List<String> getDomainsStatus(String srcPath) throws IOException {
        List<String> domainsStatus = new ArrayList<String>();
        // 获取域名
        Configuration conf = new Configuration();
        // conf.set("fs.default.name", "hdfs://192.168.100.203:8020");
        FileSystem fs = FileSystem.get(conf);
        Path dir = new Path(srcPath);
        FileStatus[] fst = fs.listStatus(dir);
        for (FileStatus f : fst) {
            long logSize = 0l;
            String domain = f.getPath().getName();

            if (domain.startsWith("_SUCCESS")) {
                continue;
            }
            RemoteIterator<LocatedFileStatus> subDir = fs.listFiles(f.getPath(), true);
            while (subDir.hasNext()) {
                LocatedFileStatus lfs = subDir.next();
                String fileName = lfs.getPath().getName();

                if (!fileName.startsWith("_SUCCESS")) {
                    logSize = lfs.getLen();
                }
                break;
            }
            domainsStatus.add(domain + " 0 " + logSize);
        }
        fs.close();
        return domainsStatus;
    }

}
