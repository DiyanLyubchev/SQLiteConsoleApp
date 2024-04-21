using Microsoft.Data.Sqlite;

namespace SQLiteTriggers
{
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

        internal void CreateInitialTabels(List<string> tableColumns, List<string> indxColumns)
        {
            string query = $@"CREATE TABLE IF NOT EXISTS {tableName} (
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

        internal void CreateBackupTrigger(List<string> columnNames)
        {
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

        internal void InsertData(Dictionary<string, string> data, List<string> indexColumns)
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

        internal List<string> GetTableColumns(string tableName)
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

        internal void AddColumnIfMissing(List<string> columns, List<string> existingDbColumns, string tableName)
        {
            List<string> columnsToAdd = columns.Except(existingDbColumns).ToList();
            Console.WriteLine($"Update table: {tableName} with columns: {string.Join(',', columnsToAdd)}");
            foreach (string column in columnsToAdd)
            {
                using SqliteCommand command = connection.CreateCommand();
                command.CommandText = $"ALTER TABLE {tableName} ADD COLUMN {column} TEXT";
                command.ExecuteNonQuery();
            }
        }

        internal int GetRecordCount()
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
    }
}
