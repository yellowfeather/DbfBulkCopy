using CommandLine;
using CommandLine.Text;
using System;
using System.Data.SqlClient;
using System.Diagnostics;

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
            Console.WriteLine($"  UserID: {options.UserId}");
            Console.WriteLine($"  Truncate: {options.Truncate}");
            Console.WriteLine();

            var connectionString = BuildConnectionString(options);

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

        private static void DoBulkCopy(SqlConnection connection, Options options)
        {
            Console.WriteLine("Begin bulk copy");

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var rowsCopied = 0L;
            var dbfRecordCount = 0L;
            using (var dbfDataReader = new DbfDataReader.DbfDataReader(options.Dbf))
            {
                dbfRecordCount = dbfDataReader.DbfTable.Header.RecordCount;

                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.BulkCopyTimeout = options.BulkCopyTimeout;
                    bulkCopy.DestinationTableName = options.Table;

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
    }
}
