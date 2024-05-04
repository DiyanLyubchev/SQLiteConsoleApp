using Microsoft.Data.Sqlite;

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

    /// <summary>
    /// Table operations
    /// </summary>
    /// <param name="tempTableName"></param>
    internal void CreateInitialTabels(string tempTableName)
    {
        List<string> tableColumns = GetTableColumns(tempTableName);

        string query = $@"
                 CREATE TABLE IF NOT EXISTS {this.tableName} (
                    ID INTEGER PRIMARY KEY,
                             {string.Join(" TEXT,", tableColumns)} TEXT
                 ); 
             
                  CREATE TABLE IF NOT EXISTS {this.backupTableName} (
                    ID INTEGER PRIMARY KEY,
                             {string.Join(" TEXT,", tableColumns)} TEXT
                 ); 
             
                 CREATE TRIGGER IF NOT EXISTS {this.tableName}_POPULATE_ID
                 AFTER INSERT ON {this.tableName}
                 FOR EACH ROW
                 BEGIN
                     UPDATE {this.tableName}
                     SET ID_NUM = 'U_' || NEW.ID
                     WHERE ID = NEW.ID;
                 END;
             ;";

        using SqliteCommand command = connection.CreateCommand();

        command.CommandText = query;
        command.ExecuteNonQuery();
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

    internal bool TableExists(string tableName)
    {
        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = $"PRAGMA table_info('{tableName}');";
        using SqliteDataReader reader = command.ExecuteReader();
        return reader.HasRows;
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

    /// <summary>
    /// Index operations
    /// </summary>
    internal void CreateIndex(List<string> indxColumns)
    {
        using SqliteCommand command = connection.CreateCommand();
        string indexQuery = $@"CREATE UNIQUE INDEX IF NOT EXISTS {this.indexName} 
                        ON {tableName} ({string.Join(",", indxColumns)});";

        command.CommandText = indexQuery;
        command.ExecuteNonQuery();
    }

    internal void DropIndex()
    {
        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = $"DROP INDEX IF EXISTS {this.indexName}";
        command.ExecuteNonQuery();
    }

    internal List<string> GetIndexColumns()
    {
        List<string> columns = [];

        using SqliteCommand command = connection.CreateCommand();

        command.CommandText = $"PRAGMA index_info({this.indexName})";
        using SqliteDataReader reader = command.ExecuteReader();

        while (reader.Read())
        {
            string columnName = reader["name"].ToString();
            columns.Add(columnName);
        }

        return columns;
    }


    /// <summary>
    /// Trigger operations
    /// </summary>
    internal void CreateBackupTrigger()
    {
        List<string> columnNames = GetTableColumns(this.backupTableName, true);
        string columns = string.Join(",", columnNames);

        List<string> valuePlaceholders = columnNames.Select(column => $"OLD.{column}").ToList();

        string values = string.Join(",", valuePlaceholders);

        string triggerQuery = $@"CREATE TRIGGER IF NOT EXISTS {this.backupTrigger}
                             AFTER DELETE ON {this.tableName}
                             BEGIN
                                 INSERT INTO {this.backupTableName} ({columns})
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
        command.CommandText = $"DROP TRIGGER IF EXISTS {this.backupTrigger}";
        command.ExecuteNonQuery();
    }

    /// <summary>
    /// Process operations
    /// </summary>
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

    internal string InsertData(Dictionary<string, string> data, IEnumerable<string> indexColumns)
    {
        string columns = string.Join(",", data.Keys);
        string parameterNames = string.Join(",", data.Keys.Select(x => $"@{x}"));

        var parameters = data.Select(x =>
        {//TODO: Checking for null values and set parameters 
            var value = string.IsNullOrEmpty(x.Value) ? DBNull.Value : (object)x.Value;
            return new SqliteParameter($"@{x.Key}", value);
        }).ToArray();

        string whereClause = string.Join(" AND ", indexColumns.Select(col =>
        {
            string columnCheck;
            if (data.TryGetValue(col, out string value))
            {
                if (string.IsNullOrEmpty(value))
                {
                    columnCheck = $"{col} IS NULL";
                }
                else
                {
                    columnCheck = $"{col} = @{col}";
                }
            }
            else
            {
                columnCheck = $"{col} IS NULL";
            }

            return columnCheck;
        }));

        string date = DateTime.Now.ToString("yyMMdd");

        using SqliteCommand insertCommand = connection.CreateCommand();
        insertCommand.CommandText = $@"INSERT INTO {this.tableName} ({columns}) 
                                  SELECT {columns} FROM {this.backupTableName} WHERE {whereClause} 
                                  LIMIT (
                                         CASE WHEN (SELECT COUNT(*) FROM {this.tableName} WHERE {whereClause}) > 0 
                                              THEN 0 
                                              ELSE 1 
                                         END);";

        insertCommand.Parameters.AddRange(parameters);

        if (insertCommand.ExecuteNonQuery() > 0)
        {
            using SqliteCommand deleteCommand = connection.CreateCommand();
            deleteCommand.CommandText = $@"DELETE FROM {this.backupTableName} 
                                    WHERE rowid IN (
                                                    SELECT rowid FROM {this.backupTableName} 
                                                    WHERE {whereClause} LIMIT 1);";

            deleteCommand.Parameters.AddRange(parameters);
            deleteCommand.ExecuteNonQuery();
        }

        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = $@"INSERT INTO {this.tableName} ({columns})
                                 SELECT {parameterNames}
                                 WHERE NOT EXISTS (
                                     SELECT 1 FROM {this.tableName} WHERE {whereClause}
                                 ) AND NOT EXISTS (
                                     SELECT 1 FROM {this.backupTableName} WHERE {whereClause}
                                 );
                                 
                                 SELECT CASE WHEN CHANGES() > 0 THEN LAST_INSERT_ROWID() ELSE 0 END;";

        command.Parameters.AddRange(parameters);
        string upsertedId = $"U_{command.ExecuteScalar()}";

        if (upsertedId == "U_0")
        {
            using SqliteCommand updateCommand = connection.CreateCommand();
            updateCommand.CommandText = $@"UPDATE {this.tableName} 
                                           SET active = @Date
                                           WHERE {whereClause} 
                                           RETURNING ID_NUM;";

            updateCommand.Parameters.AddRange(parameters);
            updateCommand.Parameters.AddWithValue("@Date", date);

            upsertedId = updateCommand.ExecuteScalar().ToString();
        }

        return upsertedId;
    }

    [Obsolete("The new one is created!")]
    internal void InsertData(Dictionary<string, string> data, IEnumerable<string> indexColumns, bool useUpdateQuery)
    {
        string date = DateTime.Now.ToString("yyMMdd");
        List<string> columns = new(data.Keys);

        List<string> parameters = [];
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
}


