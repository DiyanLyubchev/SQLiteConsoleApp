using Dapper;
using SqliteWithDapper.Models;
using System.Data.SQLite;

namespace SqliteWithDapper.Repository
{
    public class DbRepository
    {
        private readonly SQLiteConnection connection;

        public DbRepository(SQLiteConnection connection)
        {
            this.connection = connection;
            SetupDatabase();
        }

        public void Insert(Person person, params Address[] addresses)
        {
            string insertQuery = "INSERT INTO PERSON (Name, Age) VALUES (@Name, @Age)";
            this.connection.Execute(insertQuery, person);

            long personId = connection.LastInsertRowId;

            foreach (var item in addresses)
            {
                item.PersonId = (int)personId;
            }

            var insertAddressQuery = "INSERT INTO Addresses (City, PostCode, PersonId) VALUES (@City, @PostCode, @PersonId)";
            connection.Execute(insertAddressQuery, addresses);
        }

        public IEnumerable<Person> GetAll()
                => this.connection.Query<Person>("SELECT * FROM PERSON").ToList();

        public IEnumerable<Address> GetAllAddresses()
           => this.connection.Query<Address>("SELECT * FROM Addresses").ToList();

        public (List<Person> people, List<Address> addresses) GetAllPeopleAndAddresses()
        {
            string sql = @"SELECT * FROM PERSON;
                        SELECT * FROM Addresses;";

            using var multi =  this.connection.QueryMultiple(sql);
            List<Person> people = multi.Read<Person>().ToList();
            List<Address> addresses = multi.Read<Address>().ToList();

            return (people,addresses);
        }

        public void Update(int id, string newName, int newAge)
        {
            string updateQuery = "UPDATE PERSON SET Name = @Name, Age = @Age WHERE Id = @Id";
            this.connection.Execute(updateQuery, new { Name = newName, Age = newAge, Id = id });
        }

        public void Delete(int id)
        {
            string deleteQuery = "DELETE FROM PERSON WHERE Id = @Id";
            this.connection.Execute(deleteQuery, new { Id = id });
        }

        public void DeleteAll()
        {
            connection.Execute("PRAGMA foreign_keys = ON;");

            string deleteQuery = "DELETE FROM PERSON";
            this.connection.Execute(deleteQuery);
        }

        public List<Person> GetPeopleWithAddresses()
        {
            List<Person> people = connection.Query<Person, Address, Person>(
                "SELECT p.Id, p.Name, p.Age, a.Id, a.City,  a.PostCode, a.PersonId " +
                "FROM PERSON p " +
                "LEFT JOIN Addresses a ON p.Id = a.PersonId",
                (person, address) =>
                {
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

        public void CloseConnection()
        {
            this.connection.Close();
            this.connection.Dispose();
        }

        private void SetupDatabase()
        {
            string createTabels = @"CREATE TABLE IF NOT EXISTS PERSON (
                                           Id INTEGER PRIMARY KEY AUTOINCREMENT,
                                           Name TEXT NOT NULL,
                                           Age INTEGER NOT NULL);

                                    CREATE TABLE IF NOT EXISTS Addresses (
                                           Id INTEGER PRIMARY KEY AUTOINCREMENT,
                                           City TEXT NOT NULL,
                                           PostCode INTEGER,
                                           PersonId INTEGER,
                                           FOREIGN KEY (PersonId) REFERENCES PERSON (Id) ON DELETE CASCADE);";

            using var cmd = new SQLiteCommand(createTabels, connection);
            cmd.ExecuteNonQuery();
        }
    }
}
