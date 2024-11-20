using System.Data.SQLite;

namespace SqliteWithDapper.Helper
{
    public class DbHelper : IDbHelper
    {
        private SQLiteConnection inMemoryDbConnection = null;

        public SQLiteConnection GetPhysicalDbConnection()
        {
            string currentDirectory = Directory.GetCurrentDirectory();
            string pathToDbFile = Path.Combine(currentDirectory, "..\\..\\..\\Dapper.db");

            SQLiteConnection connection = new($"Data Source={pathToDbFile};");
            connection.Open();
            return connection;
        }

        public SQLiteConnection GetInMemoryDbConnection()
        {
            if (inMemoryDbConnection == null)
            {
                inMemoryDbConnection = new($"Data Source=:memory:");
                inMemoryDbConnection.Open();
            }

            return inMemoryDbConnection;
        }
    }
}
