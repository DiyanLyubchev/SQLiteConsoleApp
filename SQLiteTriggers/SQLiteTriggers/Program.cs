// See https://aka.ms/new-console-template for more information
using Microsoft.Data.Sqlite;
using SQLiteTriggers;

string tableName = "ABC";
string tempTableName = "ABC_TEMP";
string backupTableName = $"BACKUP_{tableName}";
bool isNeedToSyncTempandMainTables = false;

SQLitePCL.raw.SetProvider(new SQLitePCL.SQLite3Provider_e_sqlite3());
//Variables for testing
bool seedRandomDate = false;
int countToInsert = 10;
bool testColumn = false;
bool testIndex = false;

List<string> tableColumns = ["ID_NUM", "ACTIVE", "MODEL", "BRAND"];
List<string> indxColumns = ["MODEL", "BRAND"];

if (testColumn)
{
    tableColumns = ["ID_NUM", "ACTIVE", "MODEL", "BRAND", "PESHO"];
}

if (testIndex)
{
    indxColumns = ["MODEL", "BRAND", "PESHO"];
    //indxColumns = ["MODEL"];
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

DbService dbService = new(connection, tableName, backupTableName);

if (!dbService.TableExists(tempTableName) && !dbService.TableExists(tableName))
{
    Console.WriteLine($"Inserting records from csv: {Path.GetFileNameWithoutExtension(pathToIportDataFile)}");

    ImportService.IportCsvFileToSQLite(pathToDbFile, tempTableName, pathToIportDataFile);
    int initialRecords = dbService.GetRecordCount(tempTableName);

    Console.WriteLine($"Inserted: {initialRecords} records into table: {tempTableName}");

    isNeedToSyncTempandMainTables = true;
}

using SqliteTransaction transaction = connection.BeginTransaction();
try
{
    dbService.CreateInitialTabels(tempTableName);
    dbService.CreateIndex(indxColumns);
    dbService.CreateBackupTrigger();

    if (isNeedToSyncTempandMainTables)
        dbService.SyncMainTableWithTemp(tempTableName);

    List<string> existingDbColumns = dbService.GetTableColumns(tableName);
    List<string> columnsToAdd = tableColumns.Except(existingDbColumns).ToList();

    if (columnsToAdd.Count > 0)
    {
        dbService.AddColumnIfMissing(columnsToAdd, tableName);
    }

    List<string> existingBackUpDbColumns = dbService.GetTableColumns(backupTableName);
    columnsToAdd = tableColumns.Except(existingBackUpDbColumns).ToList();

    if (columnsToAdd.Count > 0)
    {
        dbService.AddColumnIfMissing(columnsToAdd, backupTableName);
        dbService.DropTrigger();
        dbService.CreateBackupTrigger();
    }

    int indexColumnsCount = dbService.GetIndexColumns().Count;
    if (indexColumnsCount != indxColumns.Count)
    {
        Console.WriteLine($"Update index");
        if (indexColumnsCount > indxColumns.Count)
        {
            dbService.RemoveDuplicateRecords(indxColumns);
        }

        dbService.DropIndex();
        dbService.CreateIndex(indxColumns);
    }

    if (seedRandomDate)
    {
        ImportService.SeedRandomData(dbService, indxColumns, tableName, countToInsert);
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