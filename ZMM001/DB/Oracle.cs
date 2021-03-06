﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data.OracleClient;
using System.Data;

namespace ZMM001.DB
{
    public class Oracle
    {
        /// <summary>
        /// 取得数据库的联接，默认为取得FTS的SAP数据库联接
        /// </summary>
        /// <returns></returns>
        public static OracleConnection GetConnect()
        {
            string server = "10.11.51.100";
            string port = "1527";
            string db_name = "FTP";
            string user = "system";
            string password = "qwer1234";

            //Server=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=服务器地址)(PORT=端口号)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=数据库名称)));User Id=用户名;Password=密码;Persist Security Info=True;Enlist=true;Max Pool Size=300;Min Pool Size=0;Connection Lifetime=300;
            string connect_string = String.Format("Server=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST={0})(PORT={1})))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME={2})));User Id={3};Password={4};Persist Security Info=True;Enlist=true;Max Pool Size=300;Min Pool Size=0;Connection Lifetime=300;", server, port, db_name, user, password);

            OracleConnection conn = new OracleConnection(connect_string);
            try
            {
                conn.Open();
                return conn;
            }
            catch (Exception)
            {
                throw;
            }
        }


        /// <summary>
        /// 取得服务器的当前时间
        /// </summary>
        /// <returns></returns>
        public static DateTime GetServerTime()
        {
            string strSQL = "SELECT SYSDATE FROM DUAL";             // 查询Oracle的当前时间
            try 
            {
                using (OracleConnection cn = GetConnect())
                {
                    using (OracleCommand cm = new OracleCommand(strSQL, cn))
                    {
                        using (OracleDataReader dr = cm.ExecuteReader())
                        {
                            dr.Read();
                            return (DateTime)dr["SYSDATE"];
                        }
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        private static OracleConnection m_conn = null;      // 已经存在的数据库联接

        /// <summary>
        /// 取得数据库的联接
        /// </summary>
        /// <param name="server">服务器IP地址</param>
        /// <param name="port">端口</param>
        /// <param name="db_name">服务名（数据库名）</param>
        /// <param name="user">用户名</param>
        /// <param name="password">密码</param>
        /// <returns></returns>
        public static OracleConnection GetConnect(string server, string port, string db_name, string user, string password)
        {
            //Server=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=服务器地址)(PORT=端口号)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=数据库名称)));User Id=用户名;Password=密码;Persist Security Info=True;Enlist=true;Max Pool Size=300;Min Pool Size=0;Connection Lifetime=300;
            string connect_string = String.Format("Server=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST={0})(PORT={1})))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME={2})));User Id={3};Password={4};Persist Security Info=True;Enlist=true;Max Pool Size=300;Min Pool Size=0;Connection Lifetime=300;", server, port, db_name, user, password);
            if (m_conn != null)
            {
                m_conn = new OracleConnection(connect_string);
                try
                {
                    m_conn.Open();
                }
                catch (Exception)
                {
                    m_conn = null;
                    throw;
                }
                return m_conn;
            }
            else
            {
                return m_conn;
            }
        }
        
        /// <summary>
        /// 根据传入的SQL语句，返回查询出来的数据
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static DataSet Query(string sql)
        {
            try
            {
                OracleConnection conn = Oracle.GetConnect();
                OracleCommand command = conn.CreateCommand();
                command.CommandText = sql;

                OracleDataAdapter da = new OracleDataAdapter(command);
                DataSet ds = new DataSet();
                da.Fill(ds);
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// 插入记录，不处理事务
        /// </summary>
        /// <param name="sql"></param>
        public static void BatchInsert(List<string> list)
        {
            try
            {
                OracleConnection conn = Oracle.GetConnect();
                OracleCommand command = conn.CreateCommand();
                foreach (string sql in list)
                {
                    command.CommandText = sql;
                    //Console.WriteLine(sql);
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        ///  启动一个事务。
        /// </summary>
        /// <returns></returns>
        public static OracleTransaction BeginTrans()
        {
            OracleConnection conn = Oracle.GetConnect();
            return conn.BeginTransaction();
        }

        public static void RunNonQuery(string sql)
        {
            try
            {
                using (OracleConnection con = Oracle.GetConnect())
                {
                    using (OracleCommand comm = con.CreateCommand())
                    {
                        comm.CommandText = sql;
                        comm.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}
