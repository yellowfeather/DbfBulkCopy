using CommandLine;
using CommandLine.Text;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using DbfDataReader;
using System.Data.Common;
using System.Text;
using System.Data;
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
            try
            {
                StringBuilder query = new StringBuilder();
              
                query.Append($"IF OBJECT_ID('{options.Table}', 'U') IS NOT NULL DROP TABLE {options.Table}; ");
                query.Append("CREATE TABLE ");
                query.Append(options.Table);
                query.Append(" ( ");

                var cs = dbfdr.GetColumnSchema();
                
                if (!cs.Any(col => string.Equals(col.ColumnName.ToUpperInvariant(), "ID")))
                {
                    query.Append("ID numeric(10,0) PRIMARY KEY IDENTITY(1,1) NOT NULL, ");
                }

                for (int i = 0; i < cs.Count; i++)
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
            catch(Exception ex)
            {
                Console.WriteLine($"Error creating table {ex.Message}");  
            }
        }

        private static string GetVarCharLength(DbfColumn dcol)
        {
            if (dcol.ColumnType == DbfColumnType.Memo)
            {
                return "max";
            }
            return dcol.Length != 0 ? (dcol.Length).ToString() : "max";
        }
        
        private static string ConvertToSQLType(DbColumn col)
        {
            DbfColumn dcol = col as DbfColumn;
            switch (dcol.DataType.Name)
            {
                case "String":
                    return $"varchar({GetVarCharLength(dcol)})";
                case "Int64":
                    return "bigint DEFAULT 0";
                case "Int32":
                    return "int DEFAULT 0";
                case "Boolean":
                    return "bit NOT NULL DEFAULT 0";
                case "DateTime":
                    return "Datetime";
                case "Decimal DEFAULT 0":
                    return $"Decimal({dcol.Length},{dcol.DecimalCount})";
                default:
                    return col.DataType.Name;
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
                    if (options.CreateTable)
                    {
                        CreateTable(dbfDataReader, connection, options);
                    }
                    var dt = ValidateColumns(dbfDataReader);
                    try
                    {
                        bulkCopy.WriteToServer(dt);
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

        private static DataTable ValidateColumns(DbfDataReader.DbfDataReader dbfDataReader)
        {
            var IntColumns = new List<DataColumn>();
            var DecimalColumns = new List<DataColumn>();
            var BoolColumns = new List<DataColumn>();
            var DateColumns = new List<DataColumn>();

            // Woraround to create DataTable from DataReader,
            // because dt.Fill(dbfDataReader) calls not implemented function DbfDataReader.GetValues()
            var dt = ConvertToDatatable(dbfDataReader);

            for (var i = 0; i < dt.Columns.Count; i++)
            {
                var col = dt.Columns[i];
                switch (col.DataType.Name)
                {
                    case "DateTime":
                        DateColumns.Add(col);
                        break;
                    case "Boolean":
                        BoolColumns.Add(col);
                        break;
                    case "Int32":
                    case "Int64":
                    case "Decimal":
                        BoolColumns.Add(col);
                        break;
                    default:
                        break;
                }
            }

            foreach (DataRow dr in dt.Rows)
            {
                //--- convert int values
                foreach (DataColumn IntCol in IntColumns)
                {
                    if(string.IsNullOrEmpty(dr[IntCol].ToString()))
                        dr[IntCol] = DBNull.Value;
                    else 
                       dr[IntCol] = dr[IntCol];
                }
                //--- convert decimal values
                foreach (DataColumn DecCol in DecimalColumns)
                {
                    if(string.IsNullOrEmpty(dr[DecCol].ToString()))
                        dr[DecCol] = DBNull.Value; //--- this had to be set to null, not empty
                    else
                        dr[DecCol] = dr[DecCol];
                }
                //--- convert bool values
                foreach (DataColumn BoolCol in BoolColumns)
                {
                    if(string.IsNullOrEmpty(dr[BoolCol].ToString()))
                        dr[BoolCol] = 0;
                    else
                        dr[BoolCol] = dr[BoolCol];
                }
                //--- convert date values
                foreach (DataColumn DateCol in DateColumns)
                {
                    if(string.IsNullOrEmpty(dr[DateCol].ToString()))
                        dr[DateCol] = DBNull.Value;
                    else 
                        dr[DateCol] = SanitizeDateTime(dr[DateCol].ToString());
                }
            }
            return dt;
        }

        private static DataTable ConvertToDatatable(DbfDataReader.DbfDataReader dbfDataReader)
        {
            var cols = dbfDataReader.DbfTable.Columns;
            DataTable table = new DataTable();
            var cs = dbfDataReader.GetColumnSchema();
            var addIdCol = !cs.Any(col => string.Equals(col.ColumnName.ToUpperInvariant(), "ID"));
            if (addIdCol)
            {
                table.Columns.Add("ID");
            }

            for (int i = 0; i < cols.Count; i++)
            {
                var col = cols[i];
                table.Columns.Add(col.ColumnName, col.DataType);
            }

            var colsCount = addIdCol ? cols.Count + 1 : cols.Count;
            object[] values = new object[colsCount];
            while (dbfDataReader.Read())
            {
                var startIdx = addIdCol ? 1 : 0;
                for (int i = startIdx; i < values.Length; i++)
                {
                    values[i] = dbfDataReader.GetValue(i - startIdx);
                    if (string.Equals(cs[i - startIdx].DataType.Name, "String"))
                    {
                        // validate string columns
                        var length = (cs[i - startIdx] as DbfColumn).Length;
                        if (values[i] != null && !string.IsNullOrEmpty(values[i].ToString()) && values[i].ToString().Length > length)
                        {
                            // truncate string
                            values[i] = values[i].ToString()[..length].TrimEnd();
                        }
                    }
                }
                table.Rows.Add(values);
            }
            return table;
        }

        private static object SanitizeDateTime(string value)
        {
            DateTime temp;
            if(DateTime.TryParse(value, out temp))
            {
                var minDateTime = new DateTime(1753, 1, 1, 12, 0, 0);
                return temp < minDateTime ? minDateTime : temp;
            }
            return DBNull.Value;
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
