using Microsoft.Data.Sqlite;
using System.Text;

namespace SQLiteTriggers;

internal class DbService
{
    private readonly SqliteConnection connection;
    private readonly string tableName;
    private readonly string backupTableName;
    private readonly string backupTrigger;
    private readonly string indexName;
    private readonly string indexSetTableName = "INDEX_SET";
    private readonly string characterSetTableName = "CHARACTER_SET";
    private readonly string indexColumnName;
    private readonly List<string> indexColumns = [];
    private readonly List<char> cleanChars = [];

    internal DbService(SqliteConnection connection,
                       string tableName,
                       string backupTableName,
                       List<string> indexColumns,
                       string indexColumnName,
                       List<char> cleanChars)
    {
        this.connection = connection;
        this.tableName = tableName;
        this.backupTableName = backupTableName;
        this.indexName = $"{tableName}_IDX";
        this.backupTrigger = $"{tableName}_BACKUP_DELETED";
        this.indexColumns = indexColumns;
        this.indexColumnName = indexColumnName;
        this.cleanChars = cleanChars;

        if (TableExists(tableName))
        {
            UpdateIndexSetWithIndexValueIfNeeded();
        }
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

                  CREATE TABLE IF NOT EXISTS {this.indexSetTableName} (
                        ID INTEGER PRIMARY KEY,
                        INDEX_NAMES TEXT
                  );

                 CREATE TABLE IF NOT EXISTS {this.characterSetTableName} (
                        ID INTEGER PRIMARY KEY,
                        CLEAN_CHAR INTEGER
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

    internal void UpdateTableSchemaIfNeeded(List<string> tableColumns)
    {
        List<string> existingDbColumns = this.GetTableColumns(this.tableName);
        List<string> columnsToAdd = tableColumns.Select(header => header.ToUpper())
                                                .Except(existingDbColumns.Select(column => column.ToUpper()))
                                                .ToList();

        if (columnsToAdd.Count > 0)
        {
            AddColumnIfMissing(columnsToAdd, tableName);
        }

        List<string> existingBackUpDbColumns = GetTableColumns(backupTableName);
        columnsToAdd = tableColumns.Select(header => header.ToUpper())
                                   .Except(existingBackUpDbColumns.Select(column => column.ToUpper()))
                                   .ToList();

        if (columnsToAdd.Count > 0)
        {
            this.AddColumnIfMissing(columnsToAdd, backupTableName);
            this.DropTrigger();
            this.CreateBackupTrigger();
        }
    }

    internal void AddColumnIfMissing(List<string> columnsToAdd, string tableName)
    {
        Console.WriteLine($"Update table: {tableName} with columns: {string.Join(',', columnsToAdd)}");
        StringBuilder query = new();
        using SqliteCommand command = this.connection.CreateCommand();
        foreach (string column in columnsToAdd)
        {
            query.AppendLine($"ALTER TABLE {tableName} ADD COLUMN {column} TEXT;");
        }
        command.CommandText = query.ToString();
        command.ExecuteNonQuery();
    }

    /// <summary>
    /// Index operations
    /// </summary>
    internal void CreateIndex(string indexColumnName)
    {
        using SqliteCommand command = connection.CreateCommand();
        string indexQuery = $@"CREATE UNIQUE INDEX IF NOT EXISTS {this.indexName} 
                        ON {this.tableName} ({indexColumnName});";

        command.CommandText = indexQuery;
        command.ExecuteNonQuery();
    }

    private List<string> GetIndexSet()
    {
        string indexValue = null;

        using (SqliteCommand selectCommand = this.connection.CreateCommand())
        {
            selectCommand.CommandText = $"SELECT INDEX_NAMES FROM {this.indexSetTableName}";
            object result = selectCommand.ExecuteScalar();
            if (result != null)
            {
                indexValue = result.ToString();
            }
        }

        return indexValue?.Split(',').Select(x => x.Trim()).ToList();
    }

    internal void InsertInitialIndexSet()
    {
        if (GetRecordCount(this.indexSetTableName) == 0)
        {
            using (SqliteCommand insertCommand = this.connection.CreateCommand())
            {
                insertCommand.Parameters.AddWithValue("@indexColumns", string.Join(", ", this.indexColumns));

                insertCommand.CommandText = $@"INSERT INTO {this.indexSetTableName} (INDEX_NAMES) 
                                               VALUES (@indexColumns)";

                insertCommand.ExecuteNonQuery();
            }
        }
    }

    private void UpdateInitialIndexSet()
    {
        using (SqliteCommand updateCommand = this.connection.CreateCommand())
        {
            updateCommand.Parameters.AddWithValue("@indexColumns", string.Join(", ", this.indexColumns));

            updateCommand.CommandText = $@"UPDATE {this.indexSetTableName}
                                               SET INDEX_NAMES = @indexColumns;";

            updateCommand.ExecuteNonQuery();
        }
    }

    private void UpdateIndexSetWithIndexValueIfNeeded()
    {
        List<string> indexSet = GetIndexSet();
        List<string> moreIndexColumnfromExisting = this.indexColumns.Except(indexSet)
                                                                    .ToList();

        if (moreIndexColumnfromExisting.Count > 0)
        {
            this.UpdateInitialIndexSet();
            List<string> orderColumnNames = this.GetTableColumns($"{this.tableName}")
                              .Where(column => this.indexColumns.Any(x => x.Equals(column, StringComparison.OrdinalIgnoreCase)))
                              .ToList();

            UpdateIndexColumnWithNewIndexColumns(orderColumnNames);
            CleanCharVromIndexColumn(cleanChars);

            return;
        }

        List<string> lessIndexColumnfromExisting = indexSet.Except(this.indexColumns)
                                                           .ToList();
        if (lessIndexColumnfromExisting.Count > 0)
        {
            this.UpdateInitialIndexSet();
            List<string> orderColumnNames = this.GetTableColumns($"{this.tableName}")
                              .Where(column => this.indexColumns.Any(x => x.Equals(column, StringComparison.OrdinalIgnoreCase)))
                              .ToList();

            this.RemoveDuplicateRecords(orderColumnNames);

            UpdateIndexColumnWithNewIndexColumns(orderColumnNames);

            CleanCharVromIndexColumn(cleanChars);
        }
    }

    private void UpdateIndexColumnWithNewIndexColumns(List<string> orderColumnNames)
    {
        string updateIndexColumnsQuery = string.Join(" || ", orderColumnNames.Select(c => $"COALESCE({c}, '')"));

        string concatQuery = $@"UPDATE {this.tableName}
                                       SET {this.indexColumnName} = UPPER({updateIndexColumnsQuery});";

        using (SqliteCommand replaceCommand = this.connection.CreateCommand())
        {
            replaceCommand.CommandText = concatQuery;
            replaceCommand.ExecuteNonQuery();
        }
    }

    private void RemoveDuplicateRecords(List<string> orderColumnNames)
    {
        string updateIndexColumnsQuery = string.Join(" || ", orderColumnNames.Select(c => $"COALESCE({c}, '')"));
        string removeDuplicatesQuery = $@"
                   DELETE FROM {this.tableName}
                   WHERE ROWID NOT IN (
                       SELECT ROWID
                       FROM (
                           SELECT ROWID,
                                  ROW_NUMBER() OVER(PARTITION BY {updateIndexColumnsQuery} ORDER BY ID DESC) AS row_num
                           FROM {this.tableName}
                       ) AS ranked
                       WHERE row_num = 1
                   );";

        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = removeDuplicatesQuery;
        int recordsDeleted = command.ExecuteNonQuery();

        if (recordsDeleted > 0)
        {
            Console.WriteLine($"Moved {recordsDeleted} records to {this.backupTableName} Table");
        }
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

    internal void SyncMainTableWithTemp(string tempTableName)
    {
        string columns = string.Join(", ", GetTableColumns(tempTableName, true));

        string groupByWhereClause = $"{string.Join(" IS NOT NULL AND ", this.indexColumns)} IS NOT NULL";
        string selectWhereClause = $"{string.Join(" IS NULL OR ", this.indexColumns)} IS NULL";

        using SqliteCommand command = connection.CreateCommand();

        command.CommandText = $@"INSERT INTO {this.tableName} ({columns})
                                         WITH GROUPED_RECORDS AS (
                                                    SELECT * FROM (
                                                        SELECT * FROM {tempTableName}
                                                        WHERE {groupByWhereClause} 
                                                        GROUP BY {indexColumnName}  
                                                        HAVING MAX(ID) AND MAX(ENTRY) IS NOT NULL AND MAX(ACTIVE) IS NOT NULL 
                                                    ) AS GROUPED_RECORDS
                                                    GROUP BY ID
                                                   )
                                                   SELECT * FROM GROUPED_RECORDS
                                                   UNION
                                                   SELECT * FROM (
                                                       SELECT * FROM {tempTableName}
                                                       WHERE {selectWhereClause}
                                                   ) AS SECOND_SELECT
                                                   GROUP BY ID;";

        command.ExecuteNonQuery();

        Console.WriteLine($"Successfully moved {GetRecordCount(this.tableName)} records from {tempTableName} into {this.tableName}");
        string backupQuery = $@"
                            INSERT INTO {this.backupTableName} ({columns})
                            SELECT * FROM {tempTableName}
                            WHERE ID NOT IN (
                                SELECT ID FROM (
                                    WITH GROUPED_RECORDS AS (
                                        SELECT ID FROM (
                                            SELECT ID FROM {tempTableName}
                                            WHERE {groupByWhereClause}
                                            GROUP BY {indexColumnName}
                                            HAVING MAX(ID) AND MAX(ENTRY) IS NOT NULL AND MAX(ACTIVE) IS NOT NULL
                                        ) AS GROUPED_RECORDS
                                        GROUP BY ID
                                    )
                                    SELECT ID FROM GROUPED_RECORDS
                                    UNION
                                    SELECT ID FROM (
                                        SELECT ID FROM {tempTableName}
                                        WHERE {selectWhereClause}
                                    ) AS SECOND_SELECT
                                )
                            );
                           
                            DROP TABLE IF EXISTS {tempTableName};";

        using (SqliteCommand backupCommand = this.connection.CreateCommand())
        {
             backupCommand.CommandText = backupQuery;
             backupCommand.ExecuteNonQuery();

            int backupRecords = GetRecordCount(this.backupTableName);
            if (backupRecords > 0)
                Console.WriteLine($"Insert {backupRecords} backup records to {this.backupTableName} Table");
        }
    }

    internal string InsertData(Dictionary<string, string> data)
    {
        List<string> orderColumnNames = this.GetTableColumns($"{this.tableName}")
                                            .Where(column => indexColumns.Any(x => x.Equals(column, StringComparison.OrdinalIgnoreCase)))
                                            .Select(column => column)
                                            .ToList();

        string key = string.Join("", orderColumnNames.Select(s =>
                                                             data[s] != null
                                                             ? data[s].ToString().Trim()
                                                             : string.Empty));
        string valueForSearching = cleanChars.Aggregate(key, (str, item) => str.Replace(item.ToString(), ""))
                                             .ToUpper();

        data.Add(indexColumnName, valueForSearching);
        data.Remove("ID");

        string columns = string.Join(",", data.Keys);
        string parameterNames = string.Join(",", data.Keys.Select(x => $"@{x}"));
        var parameters = data.Select(x =>
        {
            var value = string.IsNullOrEmpty(x.Value) ? DBNull.Value : (object)x.Value;
            return new SqliteParameter($"@{x.Key}", value);
        }).ToArray();

        string whereClause = $"{indexColumnName} = @{indexColumnName}";
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
        string idNum = $"U_{command.ExecuteScalar()}";

        if (idNum == "U_0")
        {
            using SqliteCommand updateCommand = connection.CreateCommand();
            updateCommand.CommandText = $@"UPDATE {this.tableName} 
                                           SET ACTIVE = @Date
                                           WHERE {whereClause} 
                                           RETURNING ID_NUM;";

            updateCommand.Parameters.AddRange(parameters);
            updateCommand.Parameters.AddWithValue("@Date", date);

            idNum = updateCommand.ExecuteScalar().ToString();
        }

        return idNum;
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


    //Clean char process
    internal void InsertInitialCharacterSet()
    {
        int characterSetCount = this.GetRecordCount(this.characterSetTableName);
        if (characterSetCount == 0
         && this.cleanChars != null
         && this.cleanChars.Count != 0)
        {
            InsertRecordsToCharacterSetTable(this.cleanChars);
        }
    }

    private void InsertRecordsToCharacterSetTable(List<char> cleanChars)
    {
        using (SqliteCommand insertCommand = this.connection.CreateCommand())
        {
            string parameterPlaceholders = string.Join(", ", Enumerable.Range(0, cleanChars.Count)
                                                 .Select(i => $"(@cleanChar{i})"));

            insertCommand.CommandText = $@"INSERT INTO {this.characterSetTableName} (CLEAN_CHAR) 
                                                          VALUES {parameterPlaceholders}";

            for (int i = 0; i < cleanChars.Count; i++)
            {
                insertCommand.Parameters.AddWithValue($"@cleanChar{i}", (int)cleanChars[i]);
            }

            insertCommand.ExecuteNonQuery();
        }
    }

    private List<char> GetCleanCharacters()
    {
        List<char> cleanCharacters = new();

        using (SqliteCommand selectCommand = this.connection.CreateCommand())
        {
            selectCommand.CommandText = $"SELECT CLEAN_CHAR FROM {this.characterSetTableName}";
            using (SqliteDataReader reader = selectCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    int cleanCharAscii = reader.GetInt32(0);
                    char cleanChar = (char)cleanCharAscii;
                    cleanCharacters.Add(cleanChar);
                }
            }
        }

        return cleanCharacters;
    }

    internal void UpdateRecordsWithNewCharacterReplacementsIfNeeded()
    {
        if (this.cleanChars != null
         && this.cleanChars.Count != 0)
        {
            List<char> cleanCharacters = this.GetCleanCharacters();
            List<char> newCharacters = this.cleanChars.Except(cleanCharacters)
                                                      .ToList();

            if (newCharacters.Count > 0)
            {
                InsertRecordsToCharacterSetTable(newCharacters);

                CleanCharVromIndexColumn(newCharacters);
            }
        }
    }

    private void CleanCharVromIndexColumn(List<char> cleanChars)
    {
        StringBuilder replaceQueryBuilder = new(this.indexColumnName);

        foreach (char character in cleanChars)
        {
            string currentCharacter = character.ToString();

            if (currentCharacter.Equals("'"))
                currentCharacter = "''";

            replaceQueryBuilder.Insert(0, $"REPLACE(");
            replaceQueryBuilder.Append($", '{currentCharacter}', '')");
        }

        string updateQuery = $@"UPDATE {this.tableName}
                              SET {this.indexColumnName} = UPPER({replaceQueryBuilder})";

        using (SqliteCommand replaceCommand = this.connection.CreateCommand())
        {
            replaceCommand.CommandText = updateQuery;
            replaceCommand.ExecuteNonQuery();
        }
    }
}


