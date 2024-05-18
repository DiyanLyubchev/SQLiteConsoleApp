// See https://aka.ms/new-console-template for more information
using CsvHelper.Configuration;
using CsvHelper;
using Microsoft.Data.Sqlite;
using SQLiteTriggers;
using System.Globalization;

string tableName = "ABC";
string tempTableName = "ABC_TEMP";
string backupTableName = $"BACKUP_{tableName}";
bool isNeedToSyncTempandMainTables = false;
string indexColumnName = "INDEX_COLUMN";
List<char> cleanChars = ['^', ' ', '-'];

SQLitePCL.raw.SetProvider(new SQLitePCL.SQLite3Provider_e_sqlite3());
//Variables for testing
bool seedRandomDate = false;
int countToInsert = 10;
bool testColumn = false;

List<string> tableColumns = ["ID_NUM", "ENTRY", "ACTIVE", "MODEL", "BRAND"];
List<string> indxColumns = ["MODEL", "BRAND"];

if (testColumn)
{
    tableColumns = ["ID_NUM", "ENTRY", "ACTIVE", "MODEL", "BRAND", "PESHO"];
}

string currentDirectory = Directory.GetCurrentDirectory();
string pathToDbFile = Path.Combine(currentDirectory, "..\\..\\..\\Data.db");
string pathToIportDataFile = Path.Combine(currentDirectory, "..\\..\\..\\ImportData.csv");

ValidateFile(pathToIportDataFile);

SqliteConnectionStringBuilder connectionStringBuilder = new()
{
    DataSource = pathToDbFile,
    Mode = SqliteOpenMode.ReadWriteCreate,
    Cache = SqliteCacheMode.Shared
};

using SqliteConnection connection = new(connectionStringBuilder.ConnectionString);
connection.Open();

ValidateFile(pathToDbFile);

DbService dbService = new(connection,
                          tableName,
                          backupTableName,
                          indxColumns,
                          indexColumnName,
                          cleanChars);

if (!dbService.TableExists(tempTableName) && !dbService.TableExists(tableName))
{
    Console.WriteLine($"Inserting records from csv: {Path.GetFileNameWithoutExtension(pathToIportDataFile)}");

    ImportService.ImportDataFromCsv(indxColumns, indexColumnName, cleanChars, pathToDbFile, tempTableName, pathToIportDataFile);
    int initialRecords = dbService.GetRecordCount(tempTableName);

    Console.WriteLine($"Inserted: {initialRecords} records into table: {tempTableName}");

    isNeedToSyncTempandMainTables = true;
}

using SqliteTransaction transaction = connection.BeginTransaction();
try
{
    dbService.CreateInitialTabels(tempTableName);
    dbService.CreateIndex(indexColumnName);
    dbService.CreateBackupTrigger();
    dbService.InsertInitialIndexSet();
    dbService.InsertInitialCharacterSet();
    dbService.UpdateRecordsWithNewCharacterReplacementsIfNeeded();

    if (isNeedToSyncTempandMainTables)
        dbService.SyncMainTableWithTemp(tempTableName);

    dbService.UpdateTableSchemaIfNeeded(tableColumns);

    if (seedRandomDate)
    {
        ImportService.SeedRandomData(dbService, tableName, countToInsert, indexColumnName);
    }
    else if (!isNeedToSyncTempandMainTables)
    {

        using (StreamReader reader = new(pathToIportDataFile))
        {
            CsvConfiguration config = new(CultureInfo.InvariantCulture)
            {
                Delimiter = ";",
                BadDataFound = null
            };

            using (CsvReader csv = new(reader, config))
            {
                csv.Read();

                if (csv.ReadHeader())
                {
                    string[] headers = csv.HeaderRecord;

                    while (csv.Read())
                    {
                        //dynamic obj = new ExpandoObject();
                        //var objDictionary = (IDictionary<string, object>)obj;
                        Dictionary<string, string> newData = [];

                        foreach (var header in headers)
                        {
                            object value = csv.GetField(header);
                            // value = string.IsNullOrEmpty(value.ToString()) ? DBNull.Value : value;

                            newData.Add(header.ToUpper(), value?.ToString());
                        }

                        // newData = objDictionary.ToDictionary(kvp => kvp.Key, kvp => kvp.Value?.ToString());

                        if (newData.Count != 0)
                            dbService.InsertData(newData);
                    }
                }
            }
        }
    }
}
catch (Exception ex)
{
    transaction.Rollback();
    Console.WriteLine($"Error: {ex.Message}");
    Environment.Exit(1);
}

transaction.Commit();
Console.WriteLine($"Successfully process");

static void ValidateFile(string pathToIportDataFile)
{
    if (!File.Exists(pathToIportDataFile))
    {
        Console.WriteLine("The file not exist into provided location!");
        Environment.Exit(1);
    }
}