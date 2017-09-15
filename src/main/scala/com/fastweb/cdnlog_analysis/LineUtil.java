package com.fastweb.cdnlog_analysis;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;

public class LineUtil 
{
        //private static final Log LOG = LogFactory.getLog(LineUtil.class);
        private int IP_POSITION = -1;
        private int ES_POSITION = -1;
        private int SS_POSITION = -1;
        private int LT_POSITION = -1;
        private int RM_RU_RV_POSITION = -1;
        private int FS_POSITION = -1;
        private int CS_POSITION = -1;
        private int RF_POSITION = -1;
        private int UA_POSITION = -1;
        private int ST_POSITION = -1;
        private int TL_ES_POSITION = -1;
        private int DD_POSITION = -1;
        private int RK_POSITION = -1;
        private int UP_POSITION = -1;
        private int FA_POSITION = -1;
        private int FV_POSITION = -1;
        
        private String ip = null;
        private String es = null;
        private String ss = null;
        private String lt = null;
        private String rm_ru_rv = null;
        private String fs = null;
        private String cs = null;
        private String rf = null;
        private String ua = null;
        private String st = null;
        private String tl_es = null;
        private String dd = null;
        private String rk = null;
        private String up = null;
        private String fa = null;
        private String fv = null;
        
        private boolean tl_flag = false;
        private String acclog_fmt = null;
        
        public LineUtil(String acclog_fmt)
        {
                this.acclog_fmt = acclog_fmt.substring(1, acclog_fmt.length()-1).replace("\\", "");
        }
        
        private void compute_position(int pos, String field)
        {
                if(field.equals("%>a"))
                {
                        IP_POSITION = pos + (tl_flag ? 8 : 0);
                }
                else if(field.equals("%es"))
                {
                        ES_POSITION = pos + (tl_flag ? 8 : 0);
                }
                else if(field.equals("%ss"))
                {
                        SS_POSITION = pos + (tl_flag ? 8 : 0);
                }
                else if(field.equals("%lt"))
                {
                        LT_POSITION = pos + (tl_flag ? 8 : 0);
                }
                else if(field.equals("%rm %ru %rv"))
                {
                        RM_RU_RV_POSITION = pos + (tl_flag ? 8 : 0);
                }
                else if(field.equals("%Fs"))
                {
                        FS_POSITION = pos + (tl_flag ? 8 : 0);
                }
                else if(field.equals("%cs"))
                {
                        CS_POSITION = pos + (tl_flag ? 8 : 0);
                }
                else if(field.equals("%rf"))
                {
                        RF_POSITION = pos + (tl_flag ? 8 : 0);
                }
                else if(field.equals("%ua"))
                {
                        UA_POSITION = pos + (tl_flag ? 8 : 0);
                }
                else if(field.equals("%st"))
                {
                        ST_POSITION = pos + (tl_flag ? 8 : 0);
                }
                else if(field.equals("%tl"))
                {
                        tl_flag = true;
                        TL_ES_POSITION = pos + (tl_flag ? 8 : 0);
                }
                else if(field.equals("%dd"))
                {
                        DD_POSITION = pos + (tl_flag ? 8 : 0);
                }
                else if(field.equals("%rk"))
                {
                        RK_POSITION = pos + (tl_flag ? 8 : 0);
                }
                else if(field.equals("%up"))
                {
                        UP_POSITION = pos + (tl_flag ? 8 : 0);
                }
                else if(field.equals("%Fa"))
                {
                        FA_POSITION = pos + (tl_flag ? 8 : 0);
                }
                else if(field.equals("%Fv"))
                {
                        FV_POSITION = pos + (tl_flag ? 8 : 0);
                }
                else {}
        }
        
        private void set_curr_field(int pos, String field)
        {
                if(pos == this.IP_POSITION)
                {
                        ip = field;
                        return;
                }
                if(pos == this.ES_POSITION)
                {
                        es = field;
                        return;
                }
                if(pos == this.SS_POSITION)
                {
                        ss = field;
                        return;
                }
                if(pos == this.LT_POSITION)
                {
                        lt = field;
                        return;
                }
                if(pos == this.RM_RU_RV_POSITION)
                {
                        rm_ru_rv = field;
                        return;
                }
                if(pos == this.FS_POSITION)
                {
                        fs = field;
                        return;
                }
                if(pos == this.CS_POSITION)
                {
                        cs = field;
                        return;
                }
                if(pos == this.RF_POSITION)
                {
                        rf = field;
                        return;
                }
                if(pos == this.UA_POSITION)
                {
                        ua = field;
                        return;
                }
                if(pos == this.ST_POSITION)
                {
                        st = field;
                        return;
                }
                if(pos == this.TL_ES_POSITION)
                {
                        tl_es = field;
                        return;
                }
                if(pos == this.DD_POSITION)
                {
                        dd = field;
                        return;
                }
                if(pos == this.RK_POSITION)
                {
                        rk = field;
                        return;
                }
                if(pos == this.UP_POSITION)
                {
                        up = field;
                        return;
                }
                if(pos == this.FA_POSITION)
                {
                        fa = field;
                        return;
                }
                if(pos == this.FV_POSITION)
                {
                        fv = field;
                }
        }
        
        public void parse_acclog_fmt()
        {
                String line = this.acclog_fmt;
                char c;
                String s;
                int idx;
                int idx1;
                int pos = 0;
                int offset = 0;
                idx = line.indexOf(' ', offset);
                if(idx != -1)
                {
                        pos++;
                        compute_position(pos, line.substring(0, idx));
                }
                for(;;)
                {
                        if( idx == -1) break;
                        if ( idx != -1)
                        {
                                c = line.charAt(idx+1);
                                if (c == '[')
                                {
                                        idx1 = line.indexOf(']', idx);
                                        if( idx1 != -1)
                                        {
                                                pos++;
                                                compute_position(pos, line.substring(idx+2, idx1));
                                                offset = idx1;
                                        }
                                        else
                                        {
                                                return;
                                        }
                                }
                                else if (c == '\"')
                                {
                                        if( (idx1 = line.indexOf("\" ", idx+2)) != -1 || (idx1 = line.indexOf('\"', idx+2)) != -1)
                                        {
                                                pos++;
                                                compute_position(pos, line.substring(idx+2, idx1));
                                                offset = idx1;
                                        }
                                        else
                                        {
                                                return;
                                        }
                                }
                                else
                                {
                                        idx1 = line.indexOf(' ', idx+1);
                                        if( idx1 != -1)
                                        {
                                                s = line.substring(idx+1, idx1);
                                                if (s.length() > 0)
                                                {
                                                        pos++;
                                                        compute_position(pos, s);
                                                }
                                                offset = idx1;
                                        }
                                        else
                                        {
                                                offset = idx;
                                        }
                                }
                        }
                        else
                        {
                                break;
                        }
                        if (offset == -1) break;
                        idx1 = line.indexOf(' ', offset+1);
                        if(idx1 == -1) 
                        {    
                                s = line.substring(offset+1);
                                if(s.length() > 0)
                                {
                                        pos++;
                                        compute_position(pos, s);
                                }
                                break;
                        }
                        idx = line.indexOf(' ', offset);
                }
        }
        
        public void parse_line(String line)
        {
                char c;
                String s;
                int idx;
                int idx1;
                int pos = 0;
                int offset = 0;
                idx = line.indexOf(' ', offset);
                if(idx != -1)
                {
                        pos++;
                        set_curr_field(pos, line.substring(0, idx));
                }
                for(;;)
                {
                        if( idx == -1) break;
                        if ( idx != -1)
                        {
                                c = line.charAt(idx+1);
                                if (c == '[')
                                {
                                        idx1 = line.indexOf(']', idx);
                                        if( idx1 != -1)
                                        {
                                                pos++;
                                                set_curr_field(pos, line.substring(idx+2, idx1));
                                                offset = idx1;
                                        }
                                        else
                                        {
                                                return;
                                        }
                                }
                                else if (c == '\"')
                                {
                                        if( (idx1 = line.indexOf("\" ", idx+2)) != -1 || (idx1 = line.indexOf('\"', idx+2)) != -1)
                                        {
                                                pos++;
                                                set_curr_field(pos, line.substring(idx+2, idx1));
                                                offset = idx1;
                                        }
                                        else
                                        {
                                                return;
                                        }
                                }
                                else
                                {
                                        idx1 = line.indexOf(' ', idx+1);
                                        if( idx1 != -1)
                                        {
                                                s = line.substring(idx+1, idx1);
                                                if (s.length() > 0)
                                                {
                                                        pos++;
                                                        set_curr_field(pos, s);
                                                }
                                                offset = idx1;
                                        }
                                        else
                                        {
                                                offset = idx;
                                        }
                                }
                        }
                        else
                        {
                                break;
                        }
                        if (offset == -1) break;
                        idx1 = line.indexOf(' ', offset+1);
                        if(idx1 == -1) 
                        {    
                                s = line.substring(offset+1);
                                if(s.length() > 0)
                                {
                                        pos++;
                                        set_curr_field(pos, s);
                                }
                                break;
                        }
                        idx = line.indexOf(' ', offset);
                }
        }
        
        public static void main(String args[])
        {
                LineUtil lineutil = new LineUtil("\"%>a %es %ss [%lt] \\\"%rm %ru %rv\\\" %Fs %cs \\\"%rf\\\" \\\"%ua\\\" %st %tl %dd %rk %up %Fa %Fv\"");
                lineutil.parse_acclog_fmt();
                
                System.out.println("IP_POSITION = " + lineutil.IP_POSITION);
                System.out.println("ES_POSITION = " + lineutil.ES_POSITION);
                System.out.println("SS_POSITION = " + lineutil.SS_POSITION);
                System.out.println("LT_POSITION = " + lineutil.LT_POSITION);
                System.out.println("RM_RU_RV_POSITION = " + lineutil.RM_RU_RV_POSITION);
                System.out.println("FS_POSITION = " + lineutil.FS_POSITION);
                System.out.println("CS_POSITION = " + lineutil.CS_POSITION);
                System.out.println("RF_POSITION = " + lineutil.RF_POSITION);
                System.out.println("UA_POSITION = " + lineutil.UA_POSITION);
                System.out.println("ST_POSITION = " + lineutil.ST_POSITION);
                System.out.println("TL_ES_POSITION = " + lineutil.TL_ES_POSITION);
                System.out.println("DD_POSITION = " + lineutil.DD_POSITION);
                System.out.println("RK_POSITION = " + lineutil.RK_POSITION);
                System.out.println("UP_POSITION = " + lineutil.UP_POSITION);
                System.out.println("FA_POSITION = " + lineutil.FA_POSITION);
                System.out.println("FV_POSITION = " + lineutil.FV_POSITION);
                
                String line = "127.0.0.1 0.074 - [09/Apr/2015:10:09:07 +0800] \"PURGE http://a2.att.hudong.com/05/47/20300543156529142854475323467_s.jpg HTTP/1.0\" 404 294 \"-\" \"-\" FCACHE_MISS  0.000 0.000 - - - - 0.000 0.074 0.074 0 - - [127.0.0.1] \"[MISS from 118.123.114.156]\"";
                lineutil.parse_line(line);
                System.out.println("\nline = " + line);
                System.out.println("IP = " + lineutil.ip);
                System.out.println("ES = " + lineutil.es);
                System.out.println("SS = " + lineutil.ss);
                System.out.println("LT = " + lineutil.lt);
                System.out.println("RM  = " + lineutil.rm_ru_rv);
                System.out.println("FS  = " + lineutil.fs);
                System.out.println("CS  = " + lineutil.cs);
                System.out.println("RF  = " + lineutil.rf);
                System.out.println("UA = " + lineutil.ua);
                System.out.println("ST  = " + lineutil.st);
                System.out.println("TL_ES  = " + lineutil.tl_es);
                System.out.println("DD  = " + lineutil.dd);
                System.out.println("RK  = " + lineutil.rk);
                System.out.println("UP = " + lineutil.up);
                System.out.println("FA = " + lineutil.fa);
                System.out.println("FV  = " + lineutil.fv);
        }
}
