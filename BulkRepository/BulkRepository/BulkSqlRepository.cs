using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Reflection;
using System.IO;
using System.Data.SqlClient;
using System.Data;

namespace BulkRepository
{

    /// <summary>
    /// 对ado.net的批量操作
    /// </summary>
    public class BulkSqlRepository
    {


        SqlConnection connection;
        public BulkSqlRepository(string connectionString)
        { 
            connection = new SqlConnection(connectionString);
        }

        #region Fields
        /// <summary>
        /// 数据总数
        /// </summary>
        int _dataTotalCount = 0;

        /// <summary>
        /// 数据总页数
        /// </summary>
        int _dataTotalPages = 0;

        /// <summary>
        /// 数据页面大小（每次向数据库提交的记录数）
        /// </summary>
        private const int DataPageSize = 10000;
        #endregion

        #region 公用方法

        public void BulkInsert<TEntity>(IEnumerable<TEntity> item)
        {
            BulkInsert(item, false);
        }

        public void BulkInsert<TEntity>(IEnumerable<TEntity> item, bool isRemoveIdentity)
        {
            string startTag = "", endTag = "";
            if (isRemoveIdentity)
            {
                startTag = "SET IDENTITY_INSERT " + typeof(TEntity).Name + " ON;";
                endTag = "SET IDENTITY_INSERT " + typeof(TEntity).Name + "  OFF;";
            }
            DataPageProcess(item, (currentItems) =>
            {
                SqlCommand command = new SqlCommand();
                command.CommandTimeout = 0;
                command.Connection = connection;
                command.CommandText = startTag + DoSql(currentItems, SqlType.Insert) + endTag;
                command.CommandType = CommandType.Text;
                connection.Open();
                command.ExecuteNonQuery();
            });
        }

        public void BulkDelete<TEntity>(IEnumerable<TEntity> item)
        {
            DataPageProcess(item, (currentItems) =>
            {
                SqlCommand command = new SqlCommand();
                command.CommandTimeout = 0;
                command.Connection = connection;
                command.CommandText = DoSql(currentItems, SqlType.Delete);
                command.CommandType = CommandType.Text;
                connection.Open();
                command.ExecuteNonQuery();
            });

        }

        public void BulkUpdate<TEntity>(IEnumerable<TEntity> item, List<string> primaries, params string[] fieldParams)
        {
            DataPageProcess(item, (currentItems) =>
            {
                SqlCommand command = new SqlCommand();
                command.CommandTimeout = 0;
                command.Connection = connection;
                command.CommandText = DoSql(currentItems, SqlType.Update, primaries, fieldParams);
                command.CommandType = CommandType.Text;
                connection.Open();
                command.ExecuteNonQuery();
            });
        }

        #endregion

        #region Private Methods
        private static void LoggerInfo(string message)
        {

            string filePath = Path.Combine(Environment.CurrentDirectory, "Logger", "log.log");
            string dir = filePath.Substring(0, filePath.LastIndexOf("\\"));
            if (!System.IO.Directory.Exists(dir))
                System.IO.Directory.CreateDirectory(dir);

            using (System.IO.StreamWriter srFile = new System.IO.StreamWriter(filePath, true))
            {
                srFile.WriteLine(message);
                srFile.Close();
                srFile.Dispose();
            }

        }

        /// <summary>
        /// 分页进行数据提交的逻辑
        /// </summary>
        /// <param name="item">原列表</param>
        /// <param name="method">处理方法</param>
        /// <param name="currentItem">要进行处理的新列表</param>
        private void DataPageProcess<TEntity>(IEnumerable<TEntity> item, Action<IEnumerable<TEntity>> method)
        {
            if (item != null && item.Any())
            {
                _dataTotalCount = item.Count();
                this._dataTotalPages = item.Count() / DataPageSize;
                if (_dataTotalCount % DataPageSize > 0)
                    _dataTotalPages += 1;
                for (int pageIndex = 1; pageIndex <= _dataTotalPages; pageIndex++)
                {
                    var currentItems = item.Skip((pageIndex - 1) * DataPageSize).Take(DataPageSize).ToList();
                    method(currentItems);
                }
            }
        }

        private static string GetEqualStatment(string fieldName, int paramId, Type pkType)
        {
            if (pkType.IsValueType)
                return string.Format("{0} = {1}", fieldName, GetParamTag(paramId));
            return string.Format("{0} = '{1}'", fieldName, GetParamTag(paramId));

        }

        private static string GetParamTag(int paramId)
        {
            return "{" + paramId + "}";
        }


        /// <summary>
        /// 构建Update语句串
        /// 注意：如果本方法过滤了int,decimal类型更新为０的列，如果希望更新它们需要指定FieldParams参数
        /// </summary>
        /// <param name="entity">实体列表</param>
        /// <param name="fieldParams">要更新的字段</param>
        /// <returns></returns>
        private Tuple<string, object[]> CreateUpdateSql<TEntity>(TEntity entity, List<string> pkList, params string[] fieldParams)
        {
            if (entity == null)
                throw new ArgumentException("The database entity can not be null.");

            var entityType = entity.GetType();
            var tableFields = new List<PropertyInfo>();
            if (fieldParams != null && fieldParams.Count() > 0)
            {
                tableFields = entityType.GetProperties().Where(i => fieldParams.Contains(i.Name, new StringComparisonIgnoreCase())).ToList();
            }
            else
            {
                tableFields = entityType.GetProperties().Where(i =>
                              !pkList.Contains(i.Name)
                              && i.GetValue(entity, null) != null
                              && !i.PropertyType.IsEnum
                              && !(i.PropertyType == typeof(ValueType) && Convert.ToInt64(i.GetValue(entity, null)) == 0)
                              && !(i.PropertyType == typeof(DateTime) && Convert.ToDateTime(i.GetValue(entity, null)) == DateTime.MinValue)
                              && (i.PropertyType.IsValueType || i.PropertyType == typeof(string))
                              ).ToList();
            }




            //过滤主键，航行属性，状态属性等
            if (pkList == null || pkList.Count == 0)
                throw new ArgumentException("The Table entity have not a primary key.");
            var arguments = new List<object>();
            var builder = new StringBuilder();

            foreach (var change in tableFields)
            {
                if (pkList.Contains(change.Name))
                    continue;
                if (arguments.Count != 0)
                    builder.Append(", ");
                builder.Append(change.Name + " = {" + arguments.Count + "}");
                if (change.PropertyType == typeof(string)
                    || change.PropertyType == typeof(DateTime)
                    || change.PropertyType == typeof(DateTime?)
                    || change.PropertyType == typeof(bool?)
                    || change.PropertyType == typeof(bool))
                    arguments.Add("'" + change.GetValue(entity, null).ToString().Replace("'", "char(39)") + "'");
                else
                    arguments.Add(change.GetValue(entity, null));
            }

            if (builder.Length == 0)
                throw new Exception("没有任何属性进行更新");

            builder.Insert(0, " UPDATE " + string.Format("[{0}]", entityType.Name) + " SET ");

            builder.Append(" WHERE ");
            bool firstPrimaryKey = true;

            foreach (var primaryField in pkList)
            {
                if (firstPrimaryKey)
                    firstPrimaryKey = false;
                else
                    builder.Append(" AND ");

                object val = entityType.GetProperty(primaryField).GetValue(entity, null);
                Type pkType = entityType.GetProperty(primaryField).GetType();
                builder.Append(GetEqualStatment(primaryField, arguments.Count, pkType));
                arguments.Add(val);
            }
            return new Tuple<string, object[]>(builder.ToString(), arguments.ToArray());

        }

        /// <summary>
        /// 构建Delete语句串
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="entity"></param>
        /// <returns></returns>
        private Tuple<string, object[]> CreateDeleteSql<TEntity>(List<string> pkList, TEntity entity)
        {
            if (entity == null)
                throw new ArgumentException("The database entity can not be null.");

            Type entityType = entity.GetType();
          
            if (pkList == null || pkList.Count == 0)
                throw new ArgumentException("The Table entity have not a primary key.");

            var arguments = new List<object>();
            var builder = new StringBuilder();
            builder.Append(" Delete from " + string.Format("[{0}]", entityType.Name));

            builder.Append(" WHERE ");
            bool firstPrimaryKey = true;

            foreach (var primaryField in pkList)
            {
                if (firstPrimaryKey)
                    firstPrimaryKey = false;
                else
                    builder.Append(" AND ");

                Type pkType = entityType.GetProperty(primaryField).GetType();
                object val = entityType.GetProperty(primaryField).GetValue(entity, null);
                builder.Append(GetEqualStatment(primaryField, arguments.Count, pkType));
                arguments.Add(val);
            }
            return new Tuple<string, object[]>(builder.ToString(), arguments.ToArray());
        }

        /// <summary>
        /// 构建Insert语句串
        /// 主键为自增时，如果主键值为0，我们将主键插入到SQL串中
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="entity"></param>
        /// <returns></returns>
        private Tuple<string, object[]> CreateInsertSql<TEntity>(List<string> pkList,TEntity entity)
        {
            if (entity == null)
                throw new ArgumentException("The database entity can not be null.");

            Type entityType = entity.GetType();
            var table = entityType.GetProperties().Where(i => i.Name != "IsValid"
                 && i.GetValue(entity, null) != null
                 && !i.PropertyType.IsEnum
                 && (i.PropertyType.IsValueType || i.PropertyType == typeof(string))).ToArray();//过滤主键，航行属性，状态属性等

        
            var arguments = new List<object>();
            var fieldbuilder = new StringBuilder();
            var valuebuilder = new StringBuilder();

            fieldbuilder.Append(" INSERT INTO " + string.Format("[{0}]", entityType.Name) + " (");

            foreach (var member in table)
            {
                if (pkList.Contains(member.Name) && Convert.ToString(member.GetValue(entity, null)) == "0")
                    continue;
                object value = member.GetValue(entity, null);
                if (value != null)
                {
                    if (arguments.Count != 0)
                    {
                        fieldbuilder.Append(", ");
                        valuebuilder.Append(", ");
                    }

                    fieldbuilder.Append(member.Name);
                    if (member.PropertyType == typeof(string)
                        || member.PropertyType == typeof(DateTime)
                        || member.PropertyType == typeof(DateTime?)
                        || member.PropertyType == typeof(Boolean?)
                        || member.PropertyType == typeof(Boolean)
                        )
                        valuebuilder.Append("'{" + arguments.Count + "}'");
                    else
                        valuebuilder.Append("{" + arguments.Count + "}");
                    if (value is string)
                        value = value.ToString().Replace("'", "char(39)");
                    arguments.Add(value);

                }
            }


            fieldbuilder.Append(") Values (");

            fieldbuilder.Append(valuebuilder.ToString());
            fieldbuilder.Append(");");
            return new Tuple<string, object[]>(fieldbuilder.ToString(), arguments.ToArray());
        }

        /// <summary>
        /// /// <summary>
        /// 执行SQL，根据SQL操作的类型
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="list"></param>
        /// <param name="sqlType"></param>
        /// <returns></returns>
        /// </summary>
        /// <param name="list"></param>
        /// <param name="sqlType"></param>
        /// <returns></returns>
        private string DoSql<TEntity>(IEnumerable<TEntity> list, SqlType sqlType)
        {
            return DoSql(list, sqlType, null);
        }
        /// <summary>
        /// 执行SQL，根据SQL操作的类型
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="list"></param>
        /// <param name="sqlType"></param>
        /// <returns></returns>
        private string DoSql<TEntity>(IEnumerable<TEntity> list, SqlType sqlType, List<string> pkList, params string[] fieldParams)
        {
            var sqlstr = new StringBuilder();
            switch (sqlType)
            {
                case SqlType.Insert:
                    list.ToList().ForEach(i =>
                    {
                        Tuple<string, object[]> sql = CreateInsertSql(pkList,i);
                        sqlstr.AppendFormat(sql.Item1, sql.Item2);
                    });
                    break;
                case SqlType.Update:
                    list.ToList().ForEach(i =>
                    {
                        Tuple<string, object[]> sql = CreateUpdateSql(i, pkList, fieldParams);
                        sqlstr.AppendFormat(sql.Item1, sql.Item2);
                    });
                    break;
                case SqlType.Delete:
                    list.ToList().ForEach(i =>
                    {
                        Tuple<string, object[]> sql = CreateDeleteSql(pkList,i);
                        sqlstr.AppendFormat(sql.Item1, sql.Item2);
                    });
                    break;
                default:
                    throw new ArgumentException("请输入正确的参数");
            }
            return sqlstr.ToString();
        }

        /// <summary>
        /// SQL操作类型
        /// </summary>
        protected enum SqlType
        {
            Insert,
            Update,
            Delete,
        }
        #endregion

        /// <summary>
        /// 忽略大小写,作为Contaions方法的参数
        /// </summary>
        internal class StringComparisonIgnoreCase : IEqualityComparer<string>
        {

            public int GetHashCode(string t)
            {
                return t.GetHashCode();
            }
            /// <summary>
            /// 重写它的Equals，保存同时重写GetHashCode
            /// </summary>
            /// <param name="x"></param>
            /// <param name="y"></param>
            /// <returns></returns>
            public bool Equals(string x, string y)
            {
                return String.Equals((x ?? string.Empty).Trim(), (y ?? string.Empty).Trim(), StringComparison.CurrentCultureIgnoreCase);
            }

        }
    }
}
