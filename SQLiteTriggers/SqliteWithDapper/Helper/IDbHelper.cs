using System.Data.SQLite;

namespace SqliteWithDapper.Helper
{
    public interface IDbHelper
    {
        SQLiteConnection GetInMemoryDbConnection();
        SQLiteConnection GetPhysicalDbConnection();
    }
}