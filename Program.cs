using CommandLine;
using CommandLine.Text;
using System;
using System.Data.SqlClient;
using System.Diagnostics;
using DbfDataReader;
using System.Data.Common;
using System.Text;
using System.ComponentModel.DataAnnotations;
using System.Linq;

namespace DbfBulkCopy
{
    public class Program
    {
        public static int Main(string[] args)
        {
            Console.WriteLine($"");
            
            return Parser.Default.ParseArguments<Options>(args)
                .MapResult(
                options => RunAndReturnExitCode(options),
                _ => 1);
        }

        public static int RunAndReturnExitCode(Options options)
        {
            Console.WriteLine(HeadingInfo.Default);
            Console.WriteLine();
            Console.WriteLine("Bulk copy from:");
            Console.WriteLine($"  DBF: {options.Dbf}");
            Console.WriteLine("to:");
            Console.WriteLine($"  Server: {options.Server}");
            Console.WriteLine($"  Database: {options.Database}");
            Console.WriteLine($"  Table: {options.Table}");
            Console.WriteLine($"  BulkCopyTimeout: {options.BulkCopyTimeout}");
            Console.WriteLine($"  Use SSPI: {options.UseSSPI}");
            if (!options.UseSSPI)
                Console.WriteLine($"  UserID: {options.UserId}");
            Console.WriteLine($"  Truncate: {options.Truncate}");
            Console.WriteLine($"  SkipDeletedRecords: {options.SkipDeletedRecords}");
            Console.WriteLine();

            string connectionString;
            if (options.UseSSPI)
                connectionString = BuildConnectionStringSSPI(options);
            else
                connectionString = BuildConnectionString(options);

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                if (options.Truncate)
                {
                    TruncateTable(connection, options.Table);
                }

                DoBulkCopy(connection, options);
            }

            return 0;
        }

        private static void TruncateTable(SqlConnection connection, string table)
        {
            Console.WriteLine($"Truncating table '{table}'");

            var sql = $"truncate table {table};";

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var command = new SqlCommand(sql, connection);
            command.ExecuteNonQuery();

            stopwatch.Stop();

            Console.WriteLine($"Truncating table '{table}' completed in {GetElapsedTime(stopwatch)}s");
        }

        private static void CreateTable(DbfDataReader.DbfDataReader dbfdr, SqlConnection connection, Options options)
        {
            StringBuilder query = new StringBuilder();
            query.Append($"IF OBJECT_ID('{options.Table}', 'U') IS NOT NULL DROP TABLE {options.Table}; ");
            query.Append("CREATE TABLE ");
            query.Append(options.Table);
            query.Append(" ( ");

            var cs = dbfdr.GetColumnSchema();
            
            for (int i =0;i<cs.Count;i++)
            {
                var col = cs[i];
                query.Append(col.ColumnName);
                query.Append(" ");
                query.Append(ConvertToSQLType(col));
                if (i < cs.Count - 1)
                    query.Append(", ");
            }
            query.Append(")");
            SqlCommand sqlQuery = new SqlCommand(query.ToString(), connection);
            sqlQuery.ExecuteNonQuery();
        }
        
        private static string ConvertToSQLType(DbColumn col)
        {
            DbfColumn dcol = col as DbfColumn;
            switch (dcol.DataType.Name)
            {
                case "String":
                        return $"varchar(max)";
                       // return $"char({dcol.Length})";  this yields an error
                case "Int64":
                    return "bigint";
                case "Int32":
                    return "int";
                case "Boolean":
                    return "bit";
                case "DateTime":
                    return "Datetime";
                case "Decimal":
                    return $"Decimal({dcol.Length},{dcol.DecimalCount})";
                default:
                    return "x"+col.DataType.Name;
            }

        }

        private static void DoBulkCopy(SqlConnection connection, Options options)
        {
            Console.WriteLine("Begin bulk copy");

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var rowsCopied = 0L;
            var dbfRecordCount = 0L;
            var dbfDataReaderOptions = new DbfDataReaderOptions
            {
                SkipDeletedRecords = options.SkipDeletedRecords
            };
            using (var dbfDataReader = new DbfDataReader.DbfDataReader(options.Dbf, dbfDataReaderOptions))
            {
                dbfRecordCount = dbfDataReader.DbfTable.Header.RecordCount;

                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.BulkCopyTimeout = options.BulkCopyTimeout;
                    bulkCopy.DestinationTableName = options.Table;
                    CreateTable(dbfDataReader, connection, options);

                    try
                    {
                        bulkCopy.WriteToServer(dbfDataReader);
                        rowsCopied = bulkCopy.RowsCopied();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error importing: dbf file: '{options.Dbf}', exception: {ex.Message}");
                    }
                }
            }

            stopwatch.Stop();
            Console.WriteLine($"Bulk copy completed in {GetElapsedTime(stopwatch)}s");            
            Console.WriteLine($"Copied {rowsCopied} of {dbfRecordCount} rows");
        }

        private static string GetElapsedTime(Stopwatch stopwatch)
        {
            var ts = stopwatch.Elapsed;
            return $"{ts.Hours:00}:{ts.Minutes:00}:{ts.Seconds:00}.{ts.Milliseconds / 10:00}";
        }

        private static string BuildConnectionString(Options options)
        {
            return $"Server={options.Server};Database={options.Database};User ID={options.UserId};Password={options.Password};";
        }
        private static string BuildConnectionStringSSPI(Options options)
        {
            return $"Server={options.Server};Database={options.Database};Integrated Security=SSPI;";
        }

    }
}
