using Dapper;
using SqliteWithDapper.Models;
using System.Data;
using System.Data.SQLite;

namespace SqliteWithDapper.Repository
{
    public class DbRepository : IDisposable
    {
        private readonly SQLiteConnection connection;

        public DbRepository(SQLiteConnection connection)
        {   
            this.connection = connection;
            SetupDatabase();
        }

        public void Insert(Person person, params Address[] addresses)
        {
            try
            {
                string insertQuery = "INSERT INTO PERSON (Name, Age) VALUES (@Name, @Age)";
                connection.Execute(insertQuery, person);

                long personId = connection.LastInsertRowId;
                foreach (var item in addresses)
                {
                    item.PersonId = (int)personId;
                }

                string insertAddressQuery = "INSERT INTO Addresses (City, PostCode, PersonId) VALUES (@City, @PostCode, @PersonId)";
                connection.Execute(insertAddressQuery, addresses);
            }
            catch (Exception ex)
            {
                throw new DataException("Error inserting person and addresses.", ex);
            }
        }

        public IEnumerable<Person> GetAll()
            => connection.Query<Person>("SELECT * FROM PERSON").ToList();

        public IEnumerable<Address> GetAllAddresses()
            => connection.Query<Address>("SELECT * FROM Addresses").ToList();

        public (List<Person> people, List<Address> addresses) GetAllPeopleAndAddresses()
        {
            string sql = @"SELECT * FROM PERSON;
                           SELECT * FROM Addresses;";
            using var multi = connection.QueryMultiple(sql);
            List<Person> people = [.. multi.Read<Person>()];
            List<Address> addresses = [.. multi.Read<Address>()];
            return (people, addresses);
        }

        public void Update(int id, string newName, int newAge)
        {
            string updateQuery = "UPDATE PERSON SET Name = @Name, Age = @Age WHERE Id = @Id";
            connection.Execute(updateQuery, new { Name = newName, Age = newAge, Id = id });
        }

        public void Delete(int id)
        {
            string deleteQuery = "DELETE FROM PERSON WHERE Id = @Id";
            connection.Execute(deleteQuery, new { Id = id });
        }

        public void DeleteAll()
        {
            connection.Execute("PRAGMA foreign_keys = ON;");
            string deleteQuery = "DELETE FROM PERSON";
            connection.Execute(deleteQuery);
        }

        public List<Person> GetPeopleWithAddresses()
        {
            var people = connection.Query<Person, Address, Person>(
                "SELECT p.Id, p.Name, p.Age, a.Id, a.City, a.PostCode, a.PersonId " +
                "FROM PERSON p LEFT JOIN Addresses a ON p.Id = a.PersonId",
                (person, address) =>
                {
                    person.Addresses ??= new List<Address>();
                    if (address != null)
                    {
                        person.Addresses.Add(address);
                    }
                    return person;
                },
                splitOn: "Id"
            ).ToList();

            return people;
        }

        public void Dispose()
        {
            connection?.Close();
            connection?.Dispose();
            GC.SuppressFinalize(this);
        }

        private void SetupDatabase()
        {
            string createTables = @"CREATE TABLE IF NOT EXISTS PERSON (
                                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        Name TEXT NOT NULL,
                                        Age INTEGER NOT NULL);
                                    CREATE TABLE IF NOT EXISTS Addresses (
                                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        City TEXT NOT NULL,
                                        PostCode INTEGER,
                                        PersonId INTEGER,
                                        FOREIGN KEY (PersonId) REFERENCES PERSON (Id) ON DELETE CASCADE);";
            using var cmd = new SQLiteCommand(createTables, connection);
            cmd.ExecuteNonQuery();
        }
    }
}
