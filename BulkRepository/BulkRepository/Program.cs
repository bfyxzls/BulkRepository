using System;
using System.Collections.Generic;

namespace BulkRepository
{
    /// <summary>
    /// 数据表实体
    /// </summary>
    public class UserInfo
    {
        public int Id { get; set; }
        public String UserName { get; set; }
    }

    class MainClass
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            List<UserInfo> list = new List<UserInfo>();
            BulkSqlManager.Instance.BulkInsert<UserInfo>(list);
        }
    }
}
