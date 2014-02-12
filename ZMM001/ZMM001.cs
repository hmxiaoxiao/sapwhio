using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ZMM001.DB;
using System.Data;
using System.Data.OracleClient;
using System.Text.RegularExpressions;

namespace ZMM001
{
    /// <summary>
    /// 用来计算进出存报表的主要类
    /// </summary>
    public class ZMM001
    {
        private int m_year;     // 计算报表的年
        private int m_month;    // 计算报表的月
        private int m_day;      // 计算报表的日期
        private string m_account; // 帐套
        private string m_factory; // 事业部

        /// <summary>
        /// 取得默认的时间，先从服务器上取，如果取不到，则取本地时间
        /// </summary>
        private void SetQueryDefaultDateTime()
        {
            DateTime dt = DateTime.Now;
            try
            {
                dt = Oracle.GetServerTime();
            }
            catch (Exception e)
            {
                Console.WriteLine("取服务器时间出现如下错误，错误信息如下：");
                Console.WriteLine(e.ToString());
            }
            m_year = dt.Year;
            m_month = dt.Month;
            m_day = dt.Day;
        }

        /// <summary>
        /// 当只指定事业部时，别的数据取默认值
        /// </summary>
        /// <param name="factory"></param>
        public ZMM001(string factory)
        {
            m_factory = factory;
            m_account = "800";
            SetQueryDefaultDateTime();
        }


        /// <summary>
        /// 当只指定事业部、帐套时，别的数据取默认值
        /// </summary>
        /// <param name="factory"></param>
        /// <param name="account"></param>
        public ZMM001(string factory, string account)
        {
            m_factory = factory;
            m_account = account;
            SetQueryDefaultDateTime();
        }

        /// <summary>
        /// 计算指定年月日的进出存数据
        /// </summary>
        /// <param name="year">年</param>
        /// <param name="month">月</param>
        /// <param name="day">日</param>
        /// <param name="account">帐套</param>
        /// <param name="factory">事业部</param>
        public ZMM001(string factory, string account, int year, int month, int day )
        {
            m_year = year;
            m_month = month;
            m_day = day;
            m_account = account;
            m_factory = factory;
        }

        /// <summary>
        /// 取得期末的数据（注意上月的期末等于本月的期初）
        /// </summary>
        /// <param name="year">年</param>
        /// <param name="month">月</param>
        /// <param name="day">日</param>
        /// <param name="account">帐套</param>
        /// <param name="factory">工厂</param>
        /// <returns></returns>
        private DataSet GetInit(int year, int month, string account, string factory)
        {
            string strSQL = "";
            string ym = (year * 100 + month).ToString();     // 转换为201401这类的格式
    
            // 非批次库存记录
            strSQL = strSQL + " select a.matnr, c.maktx, h.mseh3, b.salk3, b.lbkum,  ";
            strSQL = strSQL + "        a.labst as q1, a.insme as q2, a.speme as q3, a.umlme as q4, ";
            strSQL = strSQL + "        a.lgort, e.lgobe, a.lfgja, a.lfmon, '' as charg, j.bkbez as wgbez, b.bklas ";
            strSQL = strSQL + " from ";
            strSQL = strSQL + " (select a.matnr, a.labst, a.insme, a.speme, a.umlme, a.lgort, a.lfgja, a.lfmon, a.mandt, a.werks";
            strSQL = strSQL + " from sapsr3.mardh a,";
            strSQL = strSQL + "     (select mandt, werks, matnr, lgort, min(lfgja*100+lfmon) as ym";
            strSQL = strSQL + "     from sapsr3.mardh";
            strSQL = strSQL + "     where (lfgja*100+lfmon)>=" + ym + " and werks = '" + factory + "' and mandt = " + account;
            strSQL = strSQL + "     group by mandt, werks,matnr, lgort) b";
            strSQL = strSQL + " where a.mandt = b.mandt And a.werks = b.werks And a.matnr = b.matnr And (a.lfgja * 100 + a.lfmon) = b.ym and a.lgort = b.lgort";
            strSQL = strSQL + " Union";
            strSQL = strSQL + " select matnr, labst, insme, speme, umlme, lgort, lfgja, lfmon, mandt, werks";
            strSQL = strSQL + " from sapsr3.mard";
            strSQL = strSQL + " where (lfgja*100+lfmon)<=" + ym + " and werks='" + factory + "' and mandt=" + account + ") a,";
    
            strSQL = strSQL + " (select a.salk3, a.lbkum, a.mandt, a.matnr, a.bwkey, a.lfgja, a.lfmon, a.bklas";
            strSQL = strSQL + "  from sapsr3.mbewh a,";
            strSQL = strSQL + "      (select mandt, bwkey, matnr, min(lfgja*100+lfmon) as ym";
            strSQL = strSQL + "      from sapsr3.mbewh";
            strSQL = strSQL + "      where mandt = " + account + " and bwkey = '" + factory + "' and (lfgja*100+lfmon)>=" + ym;
            strSQL = strSQL + "      group by mandt, bwkey, matnr) b";
            strSQL = strSQL + "  where a.mandt = b.mandt And a.bwkey = b.bwkey And a.matnr = b.matnr And (a.lfgja * 100 + a.lfmon) = b.ym";
            strSQL = strSQL + "  Union";
            strSQL = strSQL + "  select a.salk3, a.lbkum, a.mandt, a.matnr, a.bwkey, a.lfgja, a.lfmon, a.bklas";
            strSQL = strSQL + "  from sapsr3.mbew a";
            strSQL = strSQL + "  where a.mandt = " + account + " and a.bwkey = '" + factory + "' and (a.lfgja*100+a.lfmon) <= " + ym + ") b,";
    
            strSQL = strSQL + "      sapsr3.makt c, ";
            strSQL = strSQL + "      sapsr3.mara d, ";
            strSQL = strSQL + "      sapsr3.t001l e, ";
            strSQL = strSQL + "      sapsr3.t006a h, ";
            strSQL = strSQL + "      sapsr3.t025t j ";
            strSQL = strSQL + " where ";
            strSQL = strSQL + "       a.mandt = b.mandt and a.werks = b.bwkey and a.matnr = b.matnr and  ";
            strSQL = strSQL + "       b.bklas in ('8001','8002','8003','8004','8005','8006','8007','8008','8009','8011','8012','8013','8014','8015') and ";
            strSQL = strSQL + "       a.mandt = c.mandt and a.matnr = c.matnr and ";
            strSQL = strSQL + "       a.mandt = d.mandt and a.matnr = d.matnr and d.xchpf <> 'X' and ";
            strSQL = strSQL + "       e.mandt = a.mandt and e.werks = a.werks and e.lgort = a.lgort and  ";
            strSQL = strSQL + "       j.mandt = a.mandt and j.bklas = b.bklas and  ";
            strSQL = strSQL + "       h.mandt = a.mandt and h.spras = '1' and h.msehi = d.meins  ";
            strSQL = strSQL + " union ";


            // 批次管理又批次计价的物料
            strSQL = strSQL + " select a.matnr,  ";
            strSQL = strSQL + "        c.maktx, h.mseh3, ";
            strSQL = strSQL + "        b.salk3, b.lbkum, ";
            strSQL = strSQL + "        a.clabs as q1, a.cinsm as q2, a.cspem as q3, 0 as q4 , ";
            strSQL = strSQL + "        a.lgort, e.lgobe, ";
            strSQL = strSQL + "        a.lfgja, a.lfmon, a.charg, j.wgbez, b.bklas  ";
            strSQL = strSQL + " from   ";
            strSQL = strSQL + "       (";
            strSQL = strSQL + "       select a.matnr, a.clabs, a.cinsm, a.cspem, a.lgort, a.lfgja, a.lfmon, a.charg, a.mandt, a.werks";
            strSQL = strSQL + "       from sapsr3.mchbh a,";
            strSQL = strSQL + "           (select mandt, werks, matnr, charg, min(lfgja*100+lfmon) as ym";
            strSQL = strSQL + "           from sapsr3.mchbh";
            strSQL = strSQL + "           where (lfgja*100+lfmon)>=" + ym + " and werks = '" + factory + "' and mandt = " + account + "";
            strSQL = strSQL + "           group by mandt, werks,matnr, charg) b";
            strSQL = strSQL + "       where a.mandt = b.mandt And a.werks = b.werks And a.matnr = b.matnr And (a.lfgja * 100 + a.lfmon) = b.ym";
            strSQL = strSQL + "           and a.charg = b.charg";
            strSQL = strSQL + "       Union";
            strSQL = strSQL + "       select a.matnr, a.clabs, a.cinsm, a.cspem, a.lgort, a.lfgja, a.lfmon, a.charg, a.mandt, a.werks";
            strSQL = strSQL + "       from sapsr3.mchb a";
            strSQL = strSQL + "       where (lfgja*100+lfmon)<=" + ym + " and werks='" + factory + "' and mandt=" + account + ") a,";
    
            strSQL = strSQL + "         (";
            strSQL = strSQL + "         select a.salk3, a.lbkum, a.mandt, a.matnr, a.bwkey, a.lfgja, a.lfmon, a.bklas, a.bwtar";
            strSQL = strSQL + "         from sapsr3.mbewh a,";
            strSQL = strSQL + "             (select mandt, bwkey, matnr, bwtar, min(lfgja*100+lfmon) as ym";
            strSQL = strSQL + "             from sapsr3.mbewh";
            strSQL = strSQL + "             where mandt = " + account + " and bwkey = '" + factory + "' and (lfgja*100+lfmon)>=" + ym + "";
            strSQL = strSQL + "             group by mandt, bwkey, matnr, bwtar) b";
            strSQL = strSQL + "         where a.mandt = b.mandt And a.bwkey = b.bwkey And a.matnr = b.matnr And (a.lfgja * 100 + a.lfmon) = b.ym";
            strSQL = strSQL + "               and a.bwtar = b.bwtar";
            strSQL = strSQL + "         Union";
            strSQL = strSQL + "         select a.salk3, a.lbkum, a.mandt, a.matnr, a.bwkey, a.lfgja, a.lfmon, a.bklas, a.bwtar";
            strSQL = strSQL + "         from sapsr3.mbew a";
            strSQL = strSQL + "         where a.mandt = " + account + " and a.bwkey = '" + factory + "' and (a.lfgja*100+a.lfmon) <= " + ym + ") b ";
    
            strSQL = strSQL + "      , sapsr3.makt c ";
            strSQL = strSQL + "      , sapsr3.mara d ";
            strSQL = strSQL + "      , sapsr3.t001l e ";
            strSQL = strSQL + "      , sapsr3.t006a h ";
            strSQL = strSQL + "      , sapsr3.t023t j ";
            strSQL = strSQL + " where  ";
            strSQL = strSQL + "       a.mandt = b.mandt and a.werks = b.bwkey  ";
            strSQL = strSQL + "       and a.matnr in (select matnr from sapsr3.mbew where mandt= " + account + " and bwkey = '" + factory + "' and bwtty = 'X' "; //'不会出现中间修改批次计价的情况;
            strSQL = strSQL + "                           and bklas in ('8001','8002','8003','8004','8005','8006','8007','8008','8009','8011','8012','8013','8014','8015')) ";
            strSQL = strSQL + "       and a.matnr = b.matnr and a.charg = b.bwtar ";
            strSQL = strSQL + "       and a.matnr = c.matnr and a.mandt = c.mandt ";
            strSQL = strSQL + "       and a.mandt = d.mandt and a.matnr = d.matnr ";
            strSQL = strSQL + "       and e.mandt = a.mandt and e.werks = a.werks and e.lgort = a.lgort ";
            strSQL = strSQL + "       and j.mandt = a.mandt and j.matkl = d.matkl ";
            strSQL = strSQL + "       and h.mandt = a.mandt and h.spras = '1' and h.msehi = d.meins  ";
            strSQL = strSQL + " union ";

            // 批次管理又非批次计价的
            strSQL = strSQL + " select a.matnr,  ";
            strSQL = strSQL + "        c.maktx, h.mseh3, ";
            strSQL = strSQL + "        b.salk3, b.lbkum, ";
            strSQL = strSQL + "        a.clabs as q1, a.cinsm as q2, a.cspem as q3, 0 as q4 , ";
            strSQL = strSQL + "        a.lgort, e.lgobe, ";
            strSQL = strSQL + "        a.lfgja, a.lfmon, a.charg, j.wgbez, b.bklas  ";
            strSQL = strSQL + " from   " ;
            strSQL = strSQL + "       (";
            strSQL = strSQL + "       select a.matnr, a.clabs, a.cinsm, a.cspem, a.lgort, a.lfgja, a.lfmon, a.charg, a.mandt, a.werks";
            strSQL = strSQL + "       from sapsr3.mchbh a,";
            strSQL = strSQL + "           (select mandt, werks, matnr, charg, min(lfgja*100+lfmon) as ym";
            strSQL = strSQL + "           from sapsr3.mchbh";
            strSQL = strSQL + "           where (lfgja*100+lfmon)>=" + ym + " and werks = '" + factory + "' and mandt = " + account + "";
            strSQL = strSQL + "           group by mandt, werks,matnr, charg) b";
            strSQL = strSQL + "       where a.mandt = b.mandt And a.werks = b.werks And a.matnr = b.matnr And (a.lfgja * 100 + a.lfmon) = b.ym";
            strSQL = strSQL + "           and a.charg = b.charg";
            strSQL = strSQL + "       Union";
            strSQL = strSQL + "       select a.matnr, a.clabs, a.cinsm, a.cspem, a.lgort, a.lfgja, a.lfmon, a.charg, a.mandt, a.werks";
            strSQL = strSQL + "       from sapsr3.mchb a";
            strSQL = strSQL + "       where (lfgja*100+lfmon)<=" + ym + " and werks='" + factory + "' and mandt=" + account + ") a,";
    
            strSQL = strSQL + "         (";
            strSQL = strSQL + "         select a.salk3, a.lbkum, a.mandt, a.matnr, a.bwkey, a.lfgja, a.lfmon, a.bklas, a.bwtar";
            strSQL = strSQL + "         from sapsr3.mbewh a,";
            strSQL = strSQL + "             (select mandt, bwkey, matnr, bwtar, min(lfgja*100+lfmon) as ym";
            strSQL = strSQL + "             from sapsr3.mbewh";
            strSQL = strSQL + "             where mandt = " + account + " and bwkey = '" + factory + "' and (lfgja*100+lfmon)>=" + ym + "";
            strSQL = strSQL + "             group by mandt, bwkey, matnr, bwtar) b";
            strSQL = strSQL + "         where a.mandt = b.mandt And a.bwkey = b.bwkey And a.matnr = b.matnr And (a.lfgja * 100 + a.lfmon) = b.ym";
            strSQL = strSQL + "               and a.bwtar = b.bwtar";
            strSQL = strSQL + "         Union";
            strSQL = strSQL + "         select a.salk3, a.lbkum, a.mandt, a.matnr, a.bwkey, a.lfgja, a.lfmon, a.bklas, a.bwtar";
            strSQL = strSQL + "         from sapsr3.mbew a";
            strSQL = strSQL + "         where a.mandt = " + account + " and a.bwkey = '" + factory + "' and (a.lfgja*100+a.lfmon) <= " + ym + ") b ";
    
            strSQL = strSQL + "      , sapsr3.makt c ";
            strSQL = strSQL + "      , sapsr3.mara d ";
            strSQL = strSQL + "      , sapsr3.t001l e ";
            strSQL = strSQL + "      , sapsr3.t006a h ";
            strSQL = strSQL + "      , sapsr3.t023t j ";
            strSQL = strSQL + " where  ";
            strSQL = strSQL + "       a.mandt = b.mandt and a.werks = b.bwkey  ";
            strSQL = strSQL + "       and a.matnr in (select matnr from sapsr3.mbew where mandt= " + account + " and bwkey = '" + factory + "' and bwtty <> 'X' " ; //'与批次计价的第一个区别（必须是非批次计价的）;
            strSQL = strSQL + "                           and bklas in ('8001','8002','8003','8004','8005','8006','8007','8008','8009','8011','8012','8013','8014','8015')) ";
            strSQL = strSQL + "       and a.matnr = b.matnr";// and a.charg = b.bwtar ";
            strSQL = strSQL + "       and a.matnr = c.matnr and a.mandt = c.mandt ";
            strSQL = strSQL + "       and a.mandt = d.mandt and a.matnr = d.matnr ";
            strSQL = strSQL + "       and e.mandt = a.mandt and e.werks = a.werks and e.lgort = a.lgort ";
            strSQL = strSQL + "       and j.mandt = a.mandt and j.matkl = d.matkl ";
            strSQL = strSQL + "       and h.mandt = a.mandt and h.spras = '1' and h.msehi = d.meins and d.xchpf = 'X' "; // 与批次计价的第二个区别（必须是批次管理的）;

    
            // 外协仓库
            strSQL = strSQL + " union ";
            strSQL = strSQL + " select a.matnr,";
            strSQL = strSQL + "        c.maktx,";
            strSQL = strSQL + "        e.mseh3,";
            strSQL = strSQL + "        b.salk3,";
            strSQL = strSQL + "        b.lbkum,";
            strSQL = strSQL + "        a.lblab as q1,";
            strSQL = strSQL + "        a.lbins as q2,";
            strSQL = strSQL + "        0       as q3,";
            strSQL = strSQL + "        0       as q4,";
            strSQL = strSQL + "        a.lifnr as lgort,";
            strSQL = strSQL + "        g.name1 as lgobe, ";
            strSQL = strSQL + "        a.lfgja,";
            strSQL = strSQL + "        a.lfmon,";
            strSQL = strSQL + "        trim(a.charg) as charg,";
            strSQL = strSQL + "        f.bkbez as wgbez,";
            strSQL = strSQL + "        b.bklas";
            strSQL = strSQL + " from";
            strSQL = strSQL + "     (select a.mandt, a.werks, a.matnr, a.lfgja, a.lfmon, a.lbins, a.lblab, a.lifnr,a.charg";
            strSQL = strSQL + "     from sapsr3.mslb a";
            strSQL = strSQL + "     where a.mandt = " + account + " and a.werks = '" + factory + "' and (a.lfgja*100+a.lfmon) <= " + ym + "";
            strSQL = strSQL + "     Union";
            strSQL = strSQL + "     select a.mandt, a.werks, a.matnr, a.lfgja, a.lfmon, a.lbins, a.lblab, a.lifnr,a.charg";
            strSQL = strSQL + "     from sapsr3.mslbh a,";
            strSQL = strSQL + "          (select mandt, werks, matnr, charg, lifnr, min(lfgja*100+lfmon) as ym";
            strSQL = strSQL + "           from sapsr3.mslbh";
            strSQL = strSQL + "           where mandt = " + account + " and werks = '" + factory + "' and lfgja*100+lfmon >= " + ym + "";
            strSQL = strSQL + "           group by mandt, werks, matnr, charg, lifnr) b";
            strSQL = strSQL + "     where a.mandt = b.mandt and a.werks = b.werks and a.matnr = b.matnr and";
            strSQL = strSQL + "           a.charg = b.charg and a.lifnr = b.lifnr and (a.lfgja*100+a.lfmon)=b.ym) a,";
            strSQL = strSQL + "           ";
            strSQL = strSQL + "     (select a.salk3, a.lbkum, a.mandt, a.matnr, a.bwkey, a.lfgja, a.lfmon, a.bklas, a.bwtar";
            strSQL = strSQL + "     from sapsr3.mbewh a,";
            strSQL = strSQL + "         (select mandt, bwkey, matnr, bwtar, min(lfgja*100+lfmon) as ym";
            strSQL = strSQL + "         from sapsr3.mbewh";
            strSQL = strSQL + "         where mandt = " + account + " and bwkey = '" + factory + "' and (lfgja*100+lfmon)>=" + ym + "";
            strSQL = strSQL + "         group by mandt, bwkey, matnr, bwtar) b";
            strSQL = strSQL + "     where a.mandt = b.mandt And a.bwkey = b.bwkey And a.matnr = b.matnr And (a.lfgja * 100 + a.lfmon) = b.ym";
            strSQL = strSQL + "           and a.bwtar = b.bwtar";
            strSQL = strSQL + "     Union";
            strSQL = strSQL + "     select a.salk3, a.lbkum, a.mandt, a.matnr, a.bwkey, a.lfgja, a.lfmon, a.bklas, a.bwtar";
            strSQL = strSQL + "     from sapsr3.mbew a";
            strSQL = strSQL + "     where a.mandt = " + account + " and a.bwkey = '" + factory + "' and (a.lfgja*100+a.lfmon) <= " + ym + ") b,";
            strSQL = strSQL + "     sapsr3.makt c,";
            strSQL = strSQL + "     sapsr3.mara d,";
            strSQL = strSQL + "     sapsr3.t006a e,";
            strSQL = strSQL + "     sapsr3.t025t f,";
            strSQL = strSQL + "     sapsr3.lfa1 g";
            strSQL = strSQL + " where a.mandt = b.mandt and a.werks = b.bwkey and a.matnr = b.matnr and a.charg = b.bwtar and";
            strSQL = strSQL + "       b.bklas in ('8001','8002','8003','8004','8005','8006','8007','8008','8009','8011','8012','8013','8014','8015') and";
            strSQL = strSQL + "       a.mandt = c.mandt and a.matnr = c.matnr and c.spras = 1 and";
            strSQL = strSQL + "       a.mandt = d.mandt and a.matnr = d.matnr and";
            strSQL = strSQL + "       a.mandt = e.mandt and e.spras = '1' and e.msehi = d.meins and";
            strSQL = strSQL + "       a.mandt = f.mandt and f.spras = '1' and f.bklas = b.bklas and";
            strSQL = strSQL + "       a.mandt = g.mandt And a.lifnr = g.lifnr";
           
            strSQL = strSQL + " order by  matnr, charg, lgort ";

            return Oracle.Query(strSQL);
        }


        /// <summary>
        /// 取得本期的入库数据
        /// </summary>
        /// <param name="year">年</param>
        /// <param name="month">月</param>
        /// <param name="day">日</param>
        /// <param name="account">帐套</param>
        /// <param name="factory">工厂</param>
        /// <param name="io_type">出入库类型</param>
        /// <returns>查询后的内容</returns>
        private DataSet GetStockIn(int year, int month, string account, string factory, string io_type)
        {
            string min_ymd;  // 查询月份的月初
            string max_ymd;  // 查询月份的下月月初时间

            string ym = (year * 100 + month).ToString();    // 生成201401这类的格式
            min_ymd = (year * 10000 + month * 100 + 1).ToString(); 
            if (month == 12)
                max_ymd = ((year + 1) * 10000 + 101).ToString();
            else
                max_ymd = (year * 10000 + (month + 1) * 100 + 1).ToString();


            string strSQL = "";

            // 541，542类似的外协移库时，仓位以外协商编号代替
            if (io_type == "541" || io_type == "542")
            {
                strSQL = strSQL + "select a.matnr,  a.lifnr as lgort, a.charg,  sum(a.menge) as menge,";
                strSQL = strSQL + "       round((case when max(b.lbkum) = 0 then avg(b.verpr) * sum(a.menge) else max(b.salk3)*sum(a.menge)/max(b.lbkum) end),4) as dmbtr";
                strSQL = strSQL + "  from sapsr3.mseg a,";
                strSQL = strSQL + "      (select matnr, bwtar, lbkum, salk3, lfgja, lfmon, verpr from sapsr3.mbew  where bwkey = '" + factory + "' AND mandt = " + account + " and (lfgja * 100 + lfmon) <= " + ym;
                strSQL = strSQL + "        and matnr in ( select matnr from sapsr3.mseg where mandt = " + account + " and werks = '" + factory + "' and bwart = '" + io_type + "' and sobkz = 'O' ";
                strSQL = strSQL + "                       and mblnr in (select mblnr from sapsr3.mkpf where mandt = " + account + " and budat >= " + min_ymd + " and budat < " + max_ymd + ") group by matnr)";
                strSQL = strSQL + " Union";
                strSQL = strSQL + " select matnr, bwtar, lbkum, salk3, lfgja, lfmon, verpr from sapsr3.mbewh where bwkey = '" + factory + "' AND mandt = " + account + " and (lfgja * 100 + lfmon) >= " + ym;
                strSQL = strSQL + "        and matnr in ( select matnr from sapsr3.mseg where mandt = " + account + " and werks = '" + factory + "' and bwart = '" + io_type + "' and sobkz = 'O' ";
                strSQL = strSQL + "                       and mblnr in (select mblnr from sapsr3.mkpf where mandt = " + account + " and budat >= " + min_ymd + " and budat < " + max_ymd + ") group by matnr)) b";
                strSQL = strSQL + " where a.mandt = " + account + " and a.werks = '" + factory + "' and a.bwart = '" + io_type + "' and a.matnr = b.matnr and a.charg = b.bwtar";
                strSQL = strSQL + " and a.matnr in (select matnr from sapsr3.mbew where mandt = " + account + " and bklas in ('8001','8002','8003','8004','8005','8006','8007','8008','8009','8011','8012','8013','8014','8015'))";
                strSQL = strSQL + "   and sobkz = 'O' and a.mblnr in (select mblnr from sapsr3.mkpf where mandt = " + account + " and budat >= " + min_ymd + " and budat < " + max_ymd + ")";
                strSQL = strSQL + "group by a.matnr, a.lifnr, a.charg";
            }
            else
            {
                strSQL = strSQL + "select matnr, lgort, charg, sum(menge) as menge, sum(dmbtr) as dmbtr ";
                strSQL = strSQL + " from sapsr3.mseg ";
                strSQL = strSQL + " where mandt = " + account + " and werks = '" + factory + "' and ";
                strSQL = strSQL + "       bwart = '" + io_type + "' and ";
                strSQL = strSQL + "       matnr in (select matnr ";
                strSQL = strSQL + "                 from sapsr3.mbew ";
                strSQL = strSQL + "                 where mandt = " + account + " and  ";
                strSQL = strSQL + "                 bklas in ('8001','8002','8003','8004','8005','8006','8007','8008','8009','8011','8012','8013','8014','8015') ";
                strSQL = strSQL + "       ) and ";
                strSQL = strSQL + "       mblnr in (select mblnr  ";
                strSQL = strSQL + "                 from sapsr3.mkpf ";
                strSQL = strSQL + "                 where mandt = " + account + " and budat >= " + min_ymd + " and budat < " + max_ymd + ") ";
                strSQL = strSQL + " group by matnr, lgort, charg ";
            }

            return Oracle.Query(strSQL);
        }


        /// <summary>
        /// 清空当前的日期
        /// </summary>
        private void ClearData()
        {
            string sql = string.Format("delete from sapsr3.zmm001 where mandt = '{0}' and werks = '{1}' and lfgja = '{2}' and lfmon = '{3}' and lfday = '{4}'",
                                        m_account, m_factory, m_year, m_month, m_day);
            Oracle.RunNonQuery(sql);
        }

        /// <summary>
        /// 分析进出库报表
        /// </summary>
        public void Run()
        {
            DateTime dtBegin = DateTime.Now;
            Console.WriteLine("开始计算事业部:{0}, 帐套:{1}, {2}年{3}月{4}日", m_factory, m_account, m_year, m_month, m_day);
            Console.WriteLine("开始时间：{0}", dtBegin.ToString());

            // 清空数据
            ClearData();

            // 保存具体的数据
            Dictionary<string, Detail> data = new Dictionary<string, Detail>();

            // 取得所有的数据
            DataSet dsEnd, dsBegin, ds101, ds102, ds122, ds123, ds161, ds162, dsX61, dsX62, ds541, ds542;
            dsEnd = this.GetInit(m_year, m_month, m_account, m_factory);
            if (m_month == 1)
                dsBegin = GetInit(m_year - 1, 12, m_account, m_factory);
            else
                dsBegin = GetInit(m_year, m_month - 1, m_account, m_factory);

            ds101 = GetStockIn(m_year, m_month, m_account, m_factory, "101");
            ds102 = GetStockIn(m_year, m_month, m_account, m_factory, "102");
            ds122 = GetStockIn(m_year, m_month, m_account, m_factory, "122");
            ds123 = GetStockIn(m_year, m_month, m_account, m_factory, "123");
            ds161 = GetStockIn(m_year, m_month, m_account, m_factory, "161");
            ds162 = GetStockIn(m_year, m_month, m_account, m_factory, "162");
            dsX61 = GetStockIn(m_year, m_month, m_account, m_factory, "X61");
            dsX62 = GetStockIn(m_year, m_month, m_account, m_factory, "X62");
            ds541 = GetStockIn(m_year, m_month, m_account, m_factory, "541");
            ds542 = GetStockIn(m_year, m_month, m_account, m_factory, "542");

            string key;
            Regex reg = new Regex("'");

            // 计算期末
            for (int i = 0; i < dsEnd.Tables[0].Rows.Count; i++)
            {

                key = dsEnd.Tables[0].Rows[i]["matnr"].ToString() + ":" +
                      dsEnd.Tables[0].Rows[i]["charg"].ToString() + ":" +
                      dsEnd.Tables[0].Rows[i]["lgort"].ToString();
                Detail detail = new Detail();
                detail.werks = m_factory;
                detail.mandt = m_account;
                detail.lfgja = m_year.ToString();
                detail.lfmon = string.Format("{0:00}", m_month);
                detail.lfday = string.Format("{0:00}", m_day);
                detail.matnr = dsEnd.Tables[0].Rows[i]["matnr"].ToString();
                detail.maktx = dsEnd.Tables[0].Rows[i]["maktx"].ToString();
                detail.bklas = dsEnd.Tables[0].Rows[i]["bklas"].ToString();
                detail.bkbez = dsEnd.Tables[0].Rows[i]["wgbez"].ToString();
                detail.meins = dsEnd.Tables[0].Rows[i]["mseh3"].ToString();
                detail.mseh3 = dsEnd.Tables[0].Rows[i]["mseh3"].ToString();
                detail.charg = dsEnd.Tables[0].Rows[i]["charg"].ToString();
                detail.lgort = dsEnd.Tables[0].Rows[i]["lgort"].ToString();
                detail.lgobe = dsEnd.Tables[0].Rows[i]["lgobe"].ToString();

                // 过滤单引号等字符
                detail.maktx = reg.Replace(detail.maktx, "''");

                detail.initq = 0;
                detail.initc = 0;
                detail.inqua = 0;
                detail.incur = 0;
                detail.outqu = 0;
                detail.outcu = 0;
                detail.endqu = decimal.Parse(dsEnd.Tables[0].Rows[i]["q1"].ToString())
                    + decimal.Parse(dsEnd.Tables[0].Rows[i]["q2"].ToString())
                    + decimal.Parse(dsEnd.Tables[0].Rows[i]["q3"].ToString())
                    + decimal.Parse(dsEnd.Tables[0].Rows[i]["q4"].ToString());
                if (detail.endqu == 0)
                    detail.endcu = 0;
                else
                    detail.endcu = decimal.Parse(dsEnd.Tables[0].Rows[i]["salk3"].ToString()) * detail.endqu / decimal.Parse(dsEnd.Tables[0].Rows[i]["lbkum"].ToString());

                data.Add(key, detail);
            }


            // 期初值
            for (int i = 0; i < dsBegin.Tables[0].Rows.Count; i++)
            {
                key = dsBegin.Tables[0].Rows[i]["matnr"].ToString() + ":" +
                    dsBegin.Tables[0].Rows[i]["charg"].ToString() + ":" +
                    dsBegin.Tables[0].Rows[i]["lgort"].ToString();
                if (!data.ContainsKey(key))
                {
                    Console.WriteLine("期初数据，找不到key：" + key);
                }
                else
                {
                    Detail d = data[key];
                    //d.initq = decimal.Parse(dsBegin.Tables[0].Rows[i]["lbkum"].ToString());
                    //d.initc = decimal.Parse(dsBegin.Tables[0].Rows[i]["salk3"].ToString());
                    d.initq = decimal.Parse(dsBegin.Tables[0].Rows[i]["q1"].ToString())
                            + decimal.Parse(dsBegin.Tables[0].Rows[i]["q2"].ToString())
                            + decimal.Parse(dsBegin.Tables[0].Rows[i]["q3"].ToString())
                            + decimal.Parse(dsBegin.Tables[0].Rows[i]["q4"].ToString());
                    if (d.initq != 0)
                        d.initc = decimal.Parse(dsBegin.Tables[0].Rows[i]["salk3"].ToString()) * d.initq / decimal.Parse(dsBegin.Tables[0].Rows[i]["lbkum"].ToString());
                    else
                        d.initc = 0;

                    // 计算出库金额
                    d.outcu = d.initc + d.incur - d.endcu;
                    d.outqu = d.initq + d.inqua - d.endqu;

                    data[key] = d;
                }
            }

            // 计算出入库
            CalcStockIn(data, ds101, "101");
            CalcStockIn(data, ds102, "102");
            CalcStockIn(data, ds122, "122");
            CalcStockIn(data, ds123, "123");
            CalcStockIn(data, ds161, "161");
            CalcStockIn(data, ds162, "162");
            CalcStockIn(data, dsX61, "X61");
            CalcStockIn(data, dsX62, "X62");
            CalcStockIn(data, ds541, "541");
            CalcStockIn(data, ds542, "542");

            // 保存到数据库里面
            string sql = "";
            List<string> list = new List<string>();
            foreach (string k in data.Keys)
            {
                Detail d = data[k];
                // 如果没有发生业务就不插入（所有的期初期末出入都为零的话）
                if (!(d.initc == 0 && d.initq == 0 && d.incur == 0 && d.inqua == 0 && d.outcu == 0 && d.outqu == 0 && d.endcu == 0 && d.endqu == 0))
                {
                    // 因为批号不能为空，所以，判断如果批号为空的话，不插入批号
                    if (d.charg == "")
                    {
                        sql = string.Format("insert into sapsr3.zmm001(werks,mandt,lfgja,lfmon,lfday,matnr,maktx,bklas,bkbez,meins,mseh3,charg,lgort,lgobe,initq,initc,inqua,incur,outqu,outcu,endqu,endcu)" +
                                             "values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}',' ','{11}','{12}',{13},{14},{15},{16},{17},{18},{19},{20})",
                                             d.werks, d.mandt, d.lfgja, d.lfmon, d.lfday, d.matnr, d.maktx, d.bklas, d.bkbez, d.meins, d.mseh3, d.lgort, d.lgobe,
                                             d.initq, d.initc, d.inqua, d.incur, d.outqu, d.outcu, d.endqu, d.endcu);
                    }
                    else
                    {
                        sql = string.Format("insert into sapsr3.zmm001(werks,mandt,lfgja,lfmon,lfday,matnr,maktx,bklas,bkbez,meins,mseh3,charg,lgort,lgobe,initq,initc,inqua,incur,outqu,outcu,endqu,endcu)" +
                                             "values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}',{14},{15},{16},{17},{18},{19},{20},{21})",
                                             d.werks, d.mandt, d.lfgja, d.lfmon, d.lfday, d.matnr, d.maktx, d.bklas, d.bkbez, d.meins, d.mseh3, d.charg, d.lgort, d.lgobe,
                                             d.initq, d.initc, d.inqua, d.incur, d.outqu, d.outcu, d.endqu, d.endcu);
                    }
                    list.Add(sql);
                }
            }
            OracleTransaction trans = Oracle.BeginTrans();
            Oracle.BatchInsert(list);
            trans.Commit();

            Console.WriteLine("计算结束，共计{0}秒。", (DateTime.Now-dtBegin).TotalSeconds);
        }

        /// <summary>
        /// 显示内容
        /// </summary>
        /// <param name="data"></param>
        private static void ShowData(Dictionary<string, Detail> data)
        {
            foreach (string k in data.Keys)
            {
                if (data[k].initq > 0 && data[k].endqu > 0)
                    Console.WriteLine("{0}, {1}, {2}, {3}", data[k].matnr, data[k].maktx, data[k].initq, data[k].endqu);
            }
        }

        /// <summary>
        /// 计算出入库数量金额
        /// </summary>
        /// <param name="data"></param>
        /// <param name="ds101"></param>
        /// <param name="tip"></param>
        private void CalcStockIn(Dictionary<string, Detail> data, DataSet ds101, string tip)
        {
            string key;
            // 各种出入库类型的统计
            for (int i = 0; i < ds101.Tables[0].Rows.Count; i++)
            {
                if (ds101.Tables[0].Rows[i]["charg"].ToString() == " ")
                {
                    key = ds101.Tables[0].Rows[i]["matnr"].ToString().Trim() + ":" +
                          ds101.Tables[0].Rows[i]["charg"].ToString().Trim() + ":" +
                          ds101.Tables[0].Rows[i]["lgort"].ToString().Trim();
                }
                else
                {
                    key = ds101.Tables[0].Rows[i]["matnr"].ToString() + ":" +
                          ds101.Tables[0].Rows[i]["charg"].ToString() + ":" +
                          ds101.Tables[0].Rows[i]["lgort"].ToString();
                }
                if (!data.ContainsKey(key))
                {
                    Console.WriteLine(tip + "入库类型，找不到key：" + key);
                }
                else
                {
                    Detail d = data[key];

                    // 设置入库数量金额
                    d.inqua += decimal.Parse(ds101.Tables[0].Rows[i]["menge"].ToString());
                    d.incur += decimal.Parse(ds101.Tables[0].Rows[i]["dmbtr"].ToString());

                    // 设置出库数量金额
                    d.outcu = d.initc + d.incur - d.endcu;
                    d.outqu = d.initq + d.inqua - d.endqu;
                    data[key] = d;
                }
            }
        }
    }


    /// <summary>
    /// 进出存的数据结构
    /// </summary>
    public struct Detail
    {
        public string werks;
        public string mandt;
        public string lfgja;
        public string lfmon;
        public string lfday;
        public string matnr;
        public string maktx;
        public string bklas;
        public string bkbez;
        public string meins;
        public string mseh3;
        public string charg;
        public string lgort;
        public string lgobe;

        public decimal initq;
        public decimal initc;
        public decimal inqua;
        public decimal incur;
        public decimal outqu;
        public decimal outcu;
        public decimal endqu;
        public decimal endcu;
    }
}
