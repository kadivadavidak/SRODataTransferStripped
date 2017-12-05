using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RSOSimplyMacDataIntegration
{
    class SqlServerConnectionHandler
    {
        internal static SqlConnection Connect()
        {
            var dbUserName = "";
            var dbPassword = "";
            var dbName = "";
            var serverName = "";

            string connetionString = $"Data Source={serverName};Initial Catalog={dbName};User ID={dbUserName};Password={dbPassword}";

            return new SqlConnection(connetionString);
        }

        internal static void SaveDataToDb(string tableName, DataTable data)
        {
            var connection = Connect();

            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                connection.Open();
                foreach (DataColumn column in data.Columns)
                {
                    bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                }
                bulkCopy.BulkCopyTimeout = 600;
                bulkCopy.DestinationTableName = $"staging.{tableName}";
                bulkCopy.BatchSize = 10000;
                bulkCopy.WriteToServer(data);
                connection.Close();
            }
        }
    }
}
