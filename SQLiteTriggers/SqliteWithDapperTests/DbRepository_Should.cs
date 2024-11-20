using SqliteWithDapper.Helper;
using SqliteWithDapper.Models;
using SqliteWithDapper.Repository;

namespace SqliteWithDapperTests
{
    public class DbRepository_Should
    {
        private IDbHelper dbHelper;

        [SetUp]
        public void Setup()
        {
            dbHelper = new DbHelper();
        }

        [Test]
        public void Constructor_ShouldNotBeNUll()
        {
            DbRepository dbRepository = new(dbHelper.GetInMemoryDbConnection());
            Assert.That(dbRepository, Is.Not.Null);
        }

        [Test]
        public void Insert_ShouldInsertCorrect()
        {
            DbRepository repository = new(dbHelper.GetInMemoryDbConnection());
            Insert(repository);

            IEnumerable<Person> people = repository.GetAll();
            IEnumerable<Address> addresses = repository.GetAllAddresses();

            Assert.Multiple(() =>
            {
                Assert.That(repository, Is.Not.Null);
                Assert.That(people, Is.Not.Null);
                Assert.That(people.Count, Does.Not.Zero);
                Assert.That(addresses, Is.Not.Null);
                Assert.That(addresses.Count, Does.Not.Zero);
            });
        }

        [Test]
        public void Delete_ShouldDeleteCascadeCorrect()
        {
            DbRepository repository = new(dbHelper.GetInMemoryDbConnection());
            Insert(repository);

            IEnumerable<Person> people = repository.GetAll();
            IEnumerable<Address> addresses = repository.GetAllAddresses();

            Assert.Multiple(() =>
            {
                Assert.That(repository, Is.Not.Null);
                Assert.That(people, Is.Not.Null);
                Assert.That(people.Count, Is.EqualTo(1));
                Assert.That(addresses, Is.Not.Null);
                Assert.That(addresses.Count, Is.EqualTo(1));
            });

            repository.DeleteAll();
            people = repository.GetAll();
            addresses = repository.GetAllAddresses();

            Assert.Multiple(() =>
            {
                Assert.That(people.Count, Is.EqualTo(0));
                Assert.That(addresses.Count, Is.EqualTo(0));
            });
        }

        [Test]
        public void GetPeopleWithAddresses_ShouldNotBeNUll()
        {
            DbRepository repository = new(dbHelper.GetInMemoryDbConnection());
            Insert(repository);

            List<Person> people = repository.GetPeopleWithAddresses();

            Assert.Multiple(() =>
            {
                Assert.That(people.First(), Is.Not.Null);
                Assert.That(people.Select(x => x.Addresses), Is.Not.Null);
            });

            IEnumerable<Person> peopleWithoutIncludeAddreses = repository.GetAll();

            Assert.That(peopleWithoutIncludeAddreses.First().Addresses, Is.Empty);
        }

        [Test]
        public void GetAllPeopleAndAddresses_ReturnTuple()
        {
            DbRepository repository = new(dbHelper.GetInMemoryDbConnection());
            Insert(repository);

            (List<Person> people, List<Address> addresses) = repository.GetAllPeopleAndAddresses();

            Assert.Multiple(() =>
            {
                Assert.That(people, Does.Not.Zero);
                Assert.That(addresses, Does.Not.Zero);
            });

            IEnumerable<Person> peopleWithoutIncludeAddreses = repository.GetAll();

            Assert.That(peopleWithoutIncludeAddreses.First().Addresses, Is.Empty);
        }

        private static void Insert(DbRepository repository)
        {
            repository.Insert(
                         new Person
                         {
                             Name = $"Pesho {Guid.NewGuid()}",
                             Age = 55
                         },
                         new Address
                         {
                             City = "Rome",
                             PostCode = 1212
                         });
        }
    }
}

