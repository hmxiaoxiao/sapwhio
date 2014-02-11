using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data.OracleClient;
using ZMM001.DB;

namespace ZMM001
{
    class Program
    {
        static void Main(string[] args)
        {



            // 处理参数
            ZMM001 zmm001 = null;
            switch (args.Length)
            {
                case 0:
                    ShowHelp();
                    break;
                case 1:
                    if (CheckFactory(args[0].ToUpper()))
                        zmm001 = new ZMM001(args[0]);
                    else
                        ShowHelp();
                    break;
                case 2:
                    if (CheckFactory(args[0].ToUpper()) && CheckAccount(args[1]))
                        zmm001 = new ZMM001(args[0], args[1]);
                    else
                        ShowHelp();
                    break;
                case 5:
                    if (CheckFactory(args[0].ToUpper()) && CheckAccount(args[1]) && CheckYMD(args[2], args[3], args[4]))
                        zmm001 = new ZMM001(args[0], args[1], int.Parse(args[2]), int.Parse(args[3]), int.Parse(args[4]));
                    else
                        ShowHelp();
                    break;
                default:
                    ShowHelp();
                    break;
            }
         

            //ZMM001 zmm001 = new ZMM001(2013, 12, 1, "800", "FTS1");
            //zmm001.Run();
            //Console.WriteLine(DateTime.Now.ToLongTimeString());
            if (zmm001 != null)
            {
                Console.WriteLine(DateTime.Now.ToString());
                zmm001.Run();
                Console.WriteLine(DateTime.Now.ToLongTimeString());
            }
            Console.WriteLine("按任意键继续");
            Console.ReadLine();
        }

        /// <summary>
        /// 检查工厂代码是否正确
        /// </summary>
        /// <param name="factory"></param>
        /// <returns></returns>
        private static bool CheckFactory(string factory)
        {
            if (!(factory == "FTS1" || factory == "FTS2" || factory == "FTS3" || factory == "FTS5" || factory == "FTS6"))
            {
                Console.WriteLine("事业部代码必须为FTS1~3, FTS5~6");
                return false;
            }
            else
            {
                return true;
            }
        }

        /// <summary>
        /// 检查帐套
        /// </summary>
        /// <param name="account"></param>
        /// <returns></returns>
        private static bool CheckAccount(string account)
        {
            if (account == "800" || account == "810")
            {
                return true;
            }
            else
            {
                Console.WriteLine("帐套必须为800或者810");
                return false;
            }
        }

        private static bool CheckYMD(string year, string month, string day)
        {
            try
            {
                int y = int.Parse(year);
                int m = int.Parse(month);
                int d = int.Parse(day);
                DateTime dt = new DateTime(y, m, d);
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("日期格式不正确");
                return false;
            }
        }

        /// <summary>
        /// 显示本程序的帮助信息
        /// </summary>
        private static void ShowHelp()
        {
            string tips = @"
本程序是用来计算SAP的进出库报表。
用法： zmm001 事业部 [帐套] [年] [月] [日]
        事业部： FTS1 - 硅一
                  FTS2 - 硅二
                  FTS3 - TE
                  FTS5 - 电镀
                  FTS6 - 洗净
                  此参数必须输入！

        帐套： 800 - 正式帐套（默认）
                810 - 测试帐套

        年、月、日： 需要计算的报表（默认为服务器年月日）
";
            Console.WriteLine(tips);
        }
    }
}
