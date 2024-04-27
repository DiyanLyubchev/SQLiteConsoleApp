﻿using Microsoft.Data.Sqlite;

namespace SQLiteTriggers;

internal class DbService
{
    private readonly SqliteConnection connection;
    private readonly string tableName;
    private readonly string backupTableName;
    private readonly string backupTrigger;
    private readonly string indexName;

    internal DbService(SqliteConnection connection, string tableName, string backupTableName)
    {
        this.connection = connection;
        this.tableName = tableName;
        this.backupTableName = backupTableName;
        this.indexName = $"{tableName}_IDX";
        this.backupTrigger = $"{tableName}_BACKUP_DELETED";
    }

    internal void CreateInitialTabels(string tempTableName, List<string> indxColumns)
    {
        List<string> tableColumns = GetTableColumns(tempTableName);

        string query = $@"
                 CREATE TABLE IF NOT EXISTS {tableName} (
                    ID INTEGER PRIMARY KEY,
                             {string.Join(" TEXT,", tableColumns)} TEXT
                 ); 
             
                  CREATE TABLE IF NOT EXISTS {backupTableName} (
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
    }

    internal void CreateBackupTrigger()
    {
        List<string> columnNames = GetTableColumns(backupTableName, true);
        string columns = string.Join(",", columnNames);

        List<string> valuePlaceholders = columnNames.Select(column => $"OLD.{column}").ToList();

        string values = string.Join(",", valuePlaceholders);

        string triggerQuery = $@"CREATE TRIGGER IF NOT EXISTS {backupTrigger}
                             AFTER DELETE ON {tableName}
                             BEGIN
                                 INSERT INTO {backupTableName} ({columns})
                                 VALUES
                                 (
                                     {values}
                                 );
                             END;";

        using SqliteCommand command = connection.CreateCommand();

        command.CommandText = triggerQuery;
        command.ExecuteNonQuery();

    }

    internal void DropTrigger()
    {
        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = $"DROP TRIGGER IF EXISTS {backupTrigger}";
        command.ExecuteNonQuery();
    }

    internal void InsertData(Dictionary<string, string> data, List<string> indexColumns, bool useUpdateQuery = false)
    {
        string date = DateTime.Now.ToString("yyMMdd");
        List<string> columns = new(data.Keys);

        List<string> parameters = new();
        foreach (var column in columns)
        {
            parameters.Add($"@{column}");
        }

        string queryNothing = $@"INSERT INTO {this.tableName} ({string.Join(",", columns)})
                                            VALUES ({string.Join(",", parameters)})
                                            ON CONFLICT({string.Join(",", indexColumns)}) 
                                            DO NOTHING";

        if (useUpdateQuery)
        {
            queryNothing = $@"INSERT INTO {this.tableName} ({string.Join(",", columns)})
                                           VALUES ({string.Join(",", parameters)})
                                           ON CONFLICT({string.Join(",", indexColumns)}) 
                                           DO UPDATE SET ACTIVE = @Date";
        }


        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = queryNothing;

        if (useUpdateQuery)
            command.Parameters.AddWithValue("@Date", date);

        foreach (var kvp in data)
        {
            command.Parameters.AddWithValue($"@{kvp.Key}", kvp.Value);
        }

        command.ExecuteNonQuery();
    }

    internal void RemoveDuplicateRecords(List<string> udatPriIndex)
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
            Console.WriteLine($"Moved {recordsDeleted} records to {backupTableName} Table");
        }
    }

    internal List<string> GetIndexColumns()
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

    internal List<string> GetTableColumns(string tableName, bool includePrimaryKey = false)
    {
        List<string> columns = [];

        using SqliteCommand command = connection.CreateCommand();

        command.CommandText = $"PRAGMA table_info({tableName})";
        using SqliteDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            string columnName = reader["name"].ToString();
            if (!columnName.ToUpper().Equals("ID", StringComparison.OrdinalIgnoreCase))
                columns.Add(columnName);

            else if (includePrimaryKey)
                columns.Add(columnName);
        }

        return columns;
    }

    internal void AddColumnIfMissing(List<string> columnsToAdd, string tableName)
    {
        Console.WriteLine($"Update table: {tableName} with columns: {string.Join(',', columnsToAdd)}");
        foreach (string column in columnsToAdd)
        {
            using SqliteCommand command = connection.CreateCommand();
            command.CommandText = $"ALTER TABLE {tableName} ADD COLUMN {column} TEXT";
            command.ExecuteNonQuery();
        }
    }

    internal int GetRecordCount(string tableName)
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

    internal bool TableExists(string tableName)
    {
        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = $"PRAGMA table_info('{tableName}');";
        using SqliteDataReader reader = command.ExecuteReader();
        return reader.HasRows;
    }

    internal void SyncMainTableWithTemp(string tempTableName)
    {
        int initialTempRecords = GetRecordCount(tempTableName);
        string columns = string.Join(", ", GetTableColumns(tempTableName, true));
        using SqliteCommand command = connection.CreateCommand();

        command.CommandText = $@"INSERT INTO {this.tableName} ({columns})
                                        SELECT * FROM {tempTableName};";

        command.ExecuteNonQuery();

        command.CommandText = $"DROP TABLE IF EXISTS {tempTableName}";
        command.ExecuteNonQuery();

        Console.WriteLine($"Successfully moved {initialTempRecords} records from {tempTableName} into {tableName}");
    }
}


