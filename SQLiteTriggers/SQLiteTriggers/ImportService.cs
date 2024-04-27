using System.Diagnostics;

namespace SQLiteTriggers;

internal class ImportService
{
    public static void IportCsvFileToSQLite(string sqliteFilePath, string tableName, string csvFilePath)
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
                                      int countToInsert)
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

            dbService.InsertData(newData, indxColumns);
        }

        Console.WriteLine($"Insert: {dbService.GetRecordCount(tableName) - initialRecords} records into table: {tableName}");
    }
}


