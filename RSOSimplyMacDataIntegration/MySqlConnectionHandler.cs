using System;
using System.Collections.Generic;
using System.Data;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace RSOSimplyMacDataIntegration
{
    class MySqlConnectionHandler
    {
        private static string IdentityColumnName { get; set; }

        internal static MySqlConnection Connect()
        {
            var connectionString = ConfigurationManager.ConnectionStrings["SimplyMacSroCs"].ConnectionString;

            var connection = new MySqlConnection
            {
                ConnectionString = connectionString
            };

            return connection;
        }

        internal static List<string> GetTableList()
        {
            var connection = Connect();

            var tblcmd = new MySqlCommand()
            {
                CommandText = "show tables from simplymac",
                Connection = connection
            };

            connection.Open();

            var tblreader = tblcmd.ExecuteReader();
            var tblCount = tblreader.FieldCount;

            var tblList = new List<string>();

            while (tblreader.Read())
            {
                for (var i = 0; i < tblCount; i++)
                {
                    tblList.Add(tblreader.GetString(i));
                }
            }

            connection.Close();

            return tblList;
        }

        internal static void GetIdentityFieldName(string tableName)
        {
            IdentityColumnName = "none";
            var connection = Connect();
            var cmd = new MySqlCommand()
            {
                CommandText = $"SELECT * FROM {tableName} LIMIT 1",
                Connection = connection
            };

            // todo: keep connection alive for entire import. (pooling)
            connection.Open();

            var reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                IdentityColumnName = reader.GetName(0);
            }

            connection.Close();
        }

        internal static int GetMaxIdentity(string tableName)
        {
            GetIdentityFieldName(tableName);

            if (IdentityColumnName == "none")
            {
                return -1;
            }
            //else if (!IdentityColumnName.ToLower().Contains("id"))
            //{
            //    // if table does not contain an ID field
            //    return 2;
            //}

            var connection = Connect();

            var cmd = new MySqlCommand()
            {
                CommandText = $"SELECT MAX({IdentityColumnName}) FROM {tableName}",
                Connection = connection
            };

            connection.Open();

            var reader = cmd.ExecuteReader();
            var maxIdentity = -1;
            bool isNumeric;

            while (reader.Read())
            {
                isNumeric = int.TryParse(reader.IsDBNull(0) ? "-1" : reader.GetString(0), out maxIdentity);
                //maxIdentity = reader.IsDBNull(0) ? -1 : Convert.ToInt32(reader.GetString(0));
            }

            connection.Close();

            if (maxIdentity <= 0)
            {
                Console.WriteLine($"Could not get maximum identity for table {tableName}.");
            }

            return maxIdentity;
        }

        internal static void SaveDateToDb(string tableName, string query)
        {
            var connection = Connect();

            var cmd = new MySqlCommand
            {
                CommandText = query,
                Connection = connection
            };

            connection.Open();

            var reader = cmd.ExecuteReader();
            var columnCount = reader.FieldCount;
            var tbl = new DataTable();

            for (var i = 0; i < columnCount; i++)
            {
                tbl.Columns.Add(reader.GetName(i));
            }

            tbl.Columns.Add("first_accessed");
            tbl.Columns.Add("last_accessed");

            while (reader.Read())
            {
                var row = tbl.NewRow();
                var i = 0;

                for (; i < columnCount; i++)
                {
                    row[i] = reader[i];
                }

                row[i] = DateTime.Now;
                row[i + 1] = DateTime.Now;

                tbl.Rows.Add(row);
            }

            connection.Close();

            SqlServerConnectionHandler.SaveDataToDb(tableName, tbl);
        }

        internal static void GetTableData(string tableName)
        {
            var maxIdentity = GetMaxIdentity(tableName);
            var currentIdentity = 1;

            if (maxIdentity == -1)
            {
                return;
            }

            string query;

            if (maxIdentity == 0)
            {
                query = $"SELECT * FROM {tableName}";

                SaveDateToDb(tableName, query);
            }
            else
            {
                while (currentIdentity <= maxIdentity)
                {
                    var maxIdentityToPull = currentIdentity + 499999;

                    query =
                        $"SELECT * FROM {tableName} WHERE {IdentityColumnName} BETWEEN {currentIdentity} AND {maxIdentityToPull}";

                    SaveDateToDb(tableName, query);

                    currentIdentity += 500000;
                }
            }
        }

        internal static void Execute()
        {
            var table = "";

            try
            {
                var tblList = GetTableList();

                foreach (var tblName in tblList)
                {
                    table = tblName;

                    GetTableData(tblName);
                }
            }
            catch (MySqlException e)
            {
                Console.WriteLine($"There was an error while getting {table} data!\n{e}");
                throw;
            }
        }
    }
}
