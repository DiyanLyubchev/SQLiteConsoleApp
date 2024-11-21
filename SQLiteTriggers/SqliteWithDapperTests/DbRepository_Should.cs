using SqliteWithDapper.Helper;
using SqliteWithDapper.Models;
using SqliteWithDapper.Repository;

namespace SqliteWithDapperTests
{
    [TestFixture]
    public class DbRepository_Should
    {
        private DbRepository repository;

        [OneTimeSetUp]
        public void Setup()
        {
            this.repository = new(new DbHelper().GetInMemoryDbConnection());
        }

        [Test]
        public void Constructor_ShouldNotBeNUll()
        {
            Assert.That(repository, Is.Not.Null);
        }

        [Test]
        public void Insert_ShouldInsertCorrect()
        {
            Insert();

            IEnumerable<Person> people = this.repository.GetAll();
            IEnumerable<Address> addresses = this.repository.GetAllAddresses();

            Assert.Multiple(() =>
            {
                Assert.That(this.repository, Is.Not.Null);
                Assert.That(people, Is.Not.Null);
                Assert.That(people.Count, Does.Not.Zero);
                Assert.That(addresses, Is.Not.Null);
                Assert.That(addresses.Count, Does.Not.Zero);
            });
        }

        [Test]
        public void Delete_ShouldDeleteCascadeCorrect()
        {
            Insert();

            IEnumerable<Person> people = this.repository.GetAll();
            IEnumerable<Address> addresses = this.repository.GetAllAddresses();

            Assert.Multiple(() =>
            {
                Assert.That(this.repository, Is.Not.Null);
                Assert.That(people, Is.Not.Null);
                Assert.That(people.Count, Is.EqualTo(1));
                Assert.That(addresses, Is.Not.Null);
                Assert.That(addresses.Count, Is.EqualTo(1));
            });

            this.repository.DeleteAll();
            people = this.repository.GetAll();
            addresses = this.repository.GetAllAddresses();

            Assert.Multiple(() =>
            {
                Assert.That(people.Count, Is.EqualTo(0));
                Assert.That(addresses.Count, Is.EqualTo(0));
            });
        }

        [Test]
        public void GetPeopleWithAddresses_ShouldNotBeNUll()
        {
            Insert();

            List<Person> people = this.repository.GetPeopleWithAddresses();

            Assert.Multiple(() =>
            {
                Assert.That(people.First(), Is.Not.Null);
                Assert.That(people.Select(x => x.Addresses), Is.Not.Null);
            });

            IEnumerable<Person> peopleWithoutIncludeAddreses = this.repository.GetAll();

            Assert.That(peopleWithoutIncludeAddreses.First().Addresses, Is.Empty);
        }

        [Test]
        public void GetAllPeopleAndAddresses_ReturnTuple()
        {
            Insert();

            (List<Person> people, List<Address> addresses) = this.repository.GetAllPeopleAndAddresses();

            Assert.Multiple(() =>
            {
                Assert.That(people, Does.Not.Zero);
                Assert.That(addresses, Does.Not.Zero);
            });

            IEnumerable<Person> peopleWithoutIncludeAddreses = this.repository.GetAll();

            Assert.That(peopleWithoutIncludeAddreses.First().Addresses, Is.Empty);
        }

        private void Insert()
        {
            this.repository.Insert(
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

