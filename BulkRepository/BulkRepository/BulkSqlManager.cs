using System;
namespace BulkRepository
{
    public class BulkSqlManager
    {
        public BulkSqlManager()
        {
        }
       
        static Object lockObj = new Object();
        static BulkSqlRepository bulkSqlRepository;
        public static BulkSqlRepository Instance
        {
            get
            {
                if (bulkSqlRepository == null)
                {
                    lock (lockObj)
                    {
                        if (bulkSqlRepository == null)
                        {
                            //这块要从配置文件读取
                            string connectionString = "Data Source=(local);Initial Catalog=Northwind;Integrated Security=true";
                            bulkSqlRepository = new BulkSqlRepository(connectionString);
                        }
                    }

                }
                return bulkSqlRepository;
            }
        }
    }
}
