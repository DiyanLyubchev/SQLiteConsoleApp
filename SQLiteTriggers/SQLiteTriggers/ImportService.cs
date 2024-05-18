using CsvHelper;
using CsvHelper.Configuration;
using System.Diagnostics;
using System.Globalization;
using System.Text;
namespace SQLiteTriggers;
internal class ImportService
{
    internal static void ImportDataFromCsv(List<string> indxColumns,
                                           string indexColumnName,
                                           List<char> cleanChars,
                                           string sqliteFilePath,
                                           string tableName,
                                           string csvImportFilePath)
    {
        string tempUdat = Path.Combine(Path.GetDirectoryName(csvImportFilePath), "Temp_Utf8.csv");
        bool isFirstRow = true;
        StringBuilder rowBuilder = new(300000);
        List<string> headers = [];

        using (StreamReader reader = new(csvImportFilePath))
        {
            CsvConfiguration config = new(CultureInfo.InvariantCulture)
            {
                Delimiter = ";",
                BadDataFound = null
            };

            using (CsvReader csv = new(reader, config))
            {
                csv.Read(); if (csv.ReadHeader())
                {
                    headers = csv.HeaderRecord.Select(x => x.ToUpper()).ToList();
                }
            }
        }

        List<int> indexColumns = [];
        List<string> orderIndexColumnNames = headers.Where(column => indxColumns.Any(x => x.Equals(column, StringComparison.OrdinalIgnoreCase)))
                                                    .Select(column => column.ToUpper())
                                                    .ToList();

        foreach (var currentColumn in orderIndexColumnNames)
        {
            indexColumns.Add(headers.IndexOf(currentColumn));
        }

        using (FileStream inputStream = new(csvImportFilePath, FileMode.Open, FileAccess.Read))
        using (FileStream outputStream = new(tempUdat, FileMode.Create, FileAccess.Write))
        {
            using StreamReader reader = new(inputStream, Encoding.Unicode);
            using StreamWriter writer = new(outputStream, Encoding.UTF8);
            while (!reader.EndOfStream)
            {
                string curentRow = reader.ReadLine();
                if (isFirstRow)
                {
                    curentRow += $";{indexColumnName}";
                    isFirstRow = false;
                }
                else
                {
                    string[] columns = curentRow.Split(';');
                    string value = string.Empty;

                    foreach (var index in indexColumns)
                    {
                        value += columns[index];
                    }

                    string cleanedValue = new(value.Trim()
                                                   .ToUpper()
                                                   .Where(c => !cleanChars.Contains(c))
                                                   .ToArray());

                    curentRow += $";{cleanedValue}";
                }

                rowBuilder.AppendLine(curentRow);
                if (rowBuilder.Length > 200000)
                {
                    writer.Write(rowBuilder.ToString());
                    rowBuilder.Clear();
                }

                if (reader.EndOfStream && rowBuilder.Length > 0)
                    writer.Write(rowBuilder.ToString());
            }
        }

        ProcessCsvFileToSQLite(sqliteFilePath, tableName, tempUdat);

        File.Delete(tempUdat);
    }

    private static void ProcessCsvFileToSQLite(string sqliteFilePath, string tableName, string csvFilePath)
    {
        ProcessStartInfo startInfo = new()
        {
            FileName = "sqlite3",
            Arguments = $"{sqliteFilePath} \".separator ;\" \".import {csvFilePath} {tableName}\"",
            RedirectStandardOutput = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        Process process = new()
        {
            StartInfo = startInfo
        };

        process.Start();
        string output = process.StandardOutput.ReadToEnd();
        process.WaitForExit();

        if (process.ExitCode != 0)
        {
            Console.WriteLine($"The import process from csv is not successful. Exit with code {process.ExitCode}");
            Environment.Exit(process.ExitCode);
        }

        process.Close();
    }

    public static void SeedRandomData(DbService dbService,
                                      List<string> indxColumns,
                                      string tableName,
                                      int countToInsert,
                                      List<char> cleanChars,
                                      string indexColumnName)
    {
        List<string> tableColumns = dbService.GetTableColumns(tableName);
        int initialRecords = dbService.GetRecordCount(tableName);
        string date = DateTime.Now.ToString("yyMMdd");

        for (int i = 0; i < countToInsert; i++)
        {
            Dictionary<string, string> newData = [];

            foreach (string column in tableColumns)
            {
                if (column.Equals("ACTIVE", StringComparison.OrdinalIgnoreCase))
                    newData.Add(column, date);
                else
                    newData.Add(column, Guid.NewGuid().ToString());
            }

            dbService.InsertData(newData, indxColumns, cleanChars, indexColumnName);
        }

        Console.WriteLine($"Insert: {dbService.GetRecordCount(tableName) - initialRecords} records into table: {tableName}");
    }
}


