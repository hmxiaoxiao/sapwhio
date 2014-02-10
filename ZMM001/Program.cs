using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data.OracleClient;

namespace ZMM001
{
    class Program
    {
        static void Main(string[] args)
        {
            //for (int i = 7; i <= 12; i++)
            //{
            //    Console.WriteLine("正在计算{0}月份的数据", i);
                Console.WriteLine(DateTime.Now.ToString());
                ZMM001 zmm001 = new ZMM001(2013, 12, 1, "800", "FTS1");
                zmm001.Run();
                Console.WriteLine(DateTime.Now.ToLongTimeString());
            //}

            Console.ReadLine();
        }
    }
}
