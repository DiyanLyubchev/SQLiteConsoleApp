// See https://aka.ms/new-console-template for more information
using Microsoft.Data.Sqlite;
string tableName = "ABC";
string indexName = $"{tableName}_IDX";

SQLitePCL.raw.SetProvider(new SQLitePCL.SQLite3Provider_e_sqlite3());

//Replace path
string path = @"..............................\SQLiteTriggers";
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

    string query = $@"CREATE TABLE IF NOT EXISTS {tableName} (
                    ID INTEGER PRIMARY KEY,
                    {string.Join(" TEXT,", tableColumns)} TEXT
        ); 

                   CREATE TABLE IF NOT EXISTS BACKUP_{tableName} (
                    ID INTEGER PRIMARY KEY,
                    {string.Join(" TEXT,", tableColumns)} TEXT
        ); 

        CREATE TRIGGER IF NOT EXISTS {tableName}_POPULATE_ID
        AFTER INSERT ON {tableName}
        FOR EACH ROW
        BEGIN
            UPDATE {tableName}
            SET ID_NUM = 'U_' || NEW.ID
            WHERE ID = NEW.ID;
        END;
    ;";

    using SqliteCommand command = connection.CreateCommand();

    command.CommandText = query;
    command.ExecuteNonQuery();

    string indexQuery = $@"CREATE UNIQUE INDEX IF NOT EXISTS {indexName} 
                        ON {tableName} ({string.Join(",", indxColumns)});";

    command.CommandText = indexQuery;
    command.ExecuteNonQuery();

    CreateBackupTrigger(tableColumns, connection);

    if (GetTableColumns(connection).Count < tableColumns.Count)
    {
        AddColumnIfMissing(tableColumns, connection);
    }

    if (GetIndexColumns(connection).Count > indxColumns.Count)
    {
        RemoveDuplicateRecords(indxColumns, connection);
    }

    if (seedDate)
    {
        int initialRecords = GetRecordCount(connection);
        int countToInsert = initialRecords + 4;
        for (int i = initialRecords; i < countToInsert; i++)
        {
            Dictionary<string, string> newData = new()
            {
                 { "MODEL", "SomeModel" },
                 { "BRAND", $"SomeBrand{i + 1}" }
            };

            InsertData(newData, indxColumns, connection);
        }
        Console.WriteLine($"Insert: {GetRecordCount(connection) - initialRecords} records into table: {tableName}");
    }

}
catch (Exception ex)
{
    transaction.Rollback();
    Console.WriteLine($"Error: {ex.Message}");
}

transaction.Commit();


void InsertData(Dictionary<string, string> data, List<string> indexColumns, SqliteConnection connection)
{

    List<string> columns = new(data.Keys);

    List<string> parameters = new();
    foreach (var column in columns)
    {
        parameters.Add($"@{column}");
    }

    string query = $"INSERT INTO {tableName} ({string.Join(",", columns)}) VALUES ({string.Join(",", parameters)}) ON CONFLICT({string.Join(",", indexColumns)}) DO NOTHING";
    using SqliteCommand command = connection.CreateCommand();
    command.CommandText = query;

    foreach (var kvp in data)
    {
        command.Parameters.AddWithValue($"@{kvp.Key}", kvp.Value);
    }

    command.ExecuteNonQuery();
}

void CreateBackupTrigger(List<string> columnNames, SqliteConnection connection)
{
    string columns = string.Join(",", columnNames);

    List<string> valuePlaceholders = new();
    foreach (var column in columnNames)
    {
        valuePlaceholders.Add($"OLD.{column}");
    }
    string values = string.Join(",", valuePlaceholders);

    string triggerQuery = $@"CREATE TRIGGER IF NOT EXISTS {tableName}_BACKUP_DELETED
                             AFTER DELETE ON {tableName}
                             BEGIN
                                 INSERT INTO BACKUP_{tableName} ({columns})
                                 VALUES
                                 (
                                     {values}
                                 );
                             END;";

    using SqliteCommand command = connection.CreateCommand();

    command.CommandText = triggerQuery;
    command.ExecuteNonQuery();

}

void RemoveDuplicateRecords(List<string> udatPriIndex, SqliteConnection connection)
{
    string removeDuplicatesQuery = $@"
             DELETE FROM {tableName}
             WHERE ROWID NOT IN (
                 SELECT ROWID
                 FROM (
                     SELECT ROWID,
                            ROW_NUMBER() OVER(PARTITION BY {string.Join(", ", udatPriIndex)} ORDER BY ID_NUM DESC) AS row_num
                     FROM {tableName}
                 ) AS ranked
                 WHERE row_num = 1
             );";

    using SqliteCommand command = connection.CreateCommand();
    command.CommandText = removeDuplicatesQuery;
    int recordsDeleted = command.ExecuteNonQuery();

    if (recordsDeleted > 0)
    {
        Console.WriteLine($"Moved {recordsDeleted} records to Backup_{tableName} Table");
    }
}

List<string> GetIndexColumns(SqliteConnection connection)
{
    List<string> columns = [];

    using SqliteCommand command = connection.CreateCommand();

    command.CommandText = $"PRAGMA index_info({indexName})";
    using SqliteDataReader reader = command.ExecuteReader();

    while (reader.Read())
    {
        string columnName = reader["name"].ToString();
        columns.Add(columnName);
    }

    return columns;
}

List<string> GetTableColumns(SqliteConnection connection)
{
    List<string> columns = [];

    using SqliteCommand command = connection.CreateCommand();

    command.CommandText = $"PRAGMA table_info({tableName})";
    using SqliteDataReader reader = command.ExecuteReader();
    while (reader.Read())
    {
        string columnName = reader["name"].ToString();
        if (!columnName.ToUpper().Equals("ID", StringComparison.OrdinalIgnoreCase))
        {
            columns.Add(columnName);
        }
    }

    return columns;
}

void AddColumnIfMissing(List<string> columns, SqliteConnection connection)
{
    List<string> existingColumns = GetTableColumns(connection);

    List<string> columnsToAdd = columns.Except(existingColumns).ToList();
    Console.WriteLine($"Update table: {tableName} with columns: {string.Join(',', columnsToAdd)}");
    foreach (string column in columnsToAdd)
    {
        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = $"ALTER TABLE {tableName} ADD COLUMN {column} TEXT";
        command.ExecuteNonQuery();
    }
}

int GetRecordCount(SqliteConnection connection)
{
    using SqliteCommand command = connection.CreateCommand();

    command.CommandText = $"SELECT COUNT(*) FROM {tableName}";

    object result = command.ExecuteScalar();
    if (result != null && result != DBNull.Value)
    {
        return Convert.ToInt32(result);
    }

    return 0;
}
