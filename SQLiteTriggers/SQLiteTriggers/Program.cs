// See https://aka.ms/new-console-template for more information
using Microsoft.Data.Sqlite;
using SQLiteTriggers;
string tableName = "ABC";
string backupTableName = $"BACKUP_{tableName}";

SQLitePCL.raw.SetProvider(new SQLitePCL.SQLite3Provider_e_sqlite3());

//Replace path
string path = @"......\SQLiteTriggers";
SqliteConnectionStringBuilder connectionStringBuilder = new()
{
    DataSource = Path.Combine(path, "Data.db"),
    Mode = SqliteOpenMode.ReadWriteCreate,
    Cache = SqliteCacheMode.Shared
};

using SqliteConnection connection = new(connectionStringBuilder.ConnectionString);
connection.Open();
using SqliteTransaction transaction = connection.BeginTransaction();

try
{
    bool seedDate = false;
    bool testColumn = false;
    bool testIndex = false;
    DbService dbService = new(connection, tableName, backupTableName);

    List<string> tableColumns = ["ID_NUM", "MODEL", "BRAND"];
    List<string> indxColumns = ["MODEL", "BRAND"];

    if (testColumn)
    {
        tableColumns = ["ID_NUM", "MODEL", "BRAND", "PESHO"];
    }

    if (testIndex)
    {
        indxColumns = ["MODEL"];
    }

    dbService.CreateInitialTabels(tableColumns, indxColumns);

    dbService.CreateBackupTrigger();

    List<string> existingDbColumns = dbService.GetTableColumns(tableName);
    List<string> columnsToAdd = tableColumns.Except(existingDbColumns).ToList();

    if (columnsToAdd.Count > 0)
    {
        dbService.AddColumnIfMissing(columnsToAdd, tableName);
    }

    List<string> existingBackDbColumns = dbService.GetTableColumns(backupTableName);
    columnsToAdd = tableColumns.Except(existingBackDbColumns).ToList();

    if (columnsToAdd.Count > 0)
    {
        dbService.AddColumnIfMissing(columnsToAdd, backupTableName);
        dbService.DropTrigger();
        dbService.CreateBackupTrigger();
    }

    if (dbService.GetIndexColumns().Count > indxColumns.Count)
    {
        dbService.RemoveDuplicateRecords(indxColumns);
    }

    if (seedDate)
    {
        int initialRecords = dbService.GetRecordCount();
        int countToInsert = initialRecords + 4;
        for (int i = initialRecords; i < countToInsert; i++)
        {
            Dictionary<string, string> newData = new()
            {
                 { "MODEL", "SomeModel" },
                 { "BRAND", $"SomeBrand{i + 1}" }
            };

            dbService.InsertData(newData, indxColumns);
        }
        Console.WriteLine($"Insert: {dbService.GetRecordCount() - initialRecords} records into table: {tableName}");
    }

}
catch (Exception ex)
{
    transaction.Rollback();
    Console.WriteLine($"Error: {ex.Message}");
}

transaction.Commit();




