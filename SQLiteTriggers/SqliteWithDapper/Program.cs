// See https://aka.ms/new-console-template for more information

using SqliteWithDapper.Helper;
using SqliteWithDapper.Models;
using SqliteWithDapper.Repository;

DbRepository repository = new(new DbHelper().GetPhysicalDbConnection());
repository.DeleteAll();

List<Person> people = repository.GetAll().ToList();
List<Address> addresses = repository.GetAllAddresses().ToList();
Console.WriteLine($"After delete People: {people.Count}");
Console.WriteLine($"After delete Addresses: {addresses.Count}");


for (int i = 0; i < 5; i++)
{
    repository.Insert(
        new Person
        {
            Name = $"Pesho {Guid.NewGuid()}",
            Age = GetRandomAge()
        },
        new Address
        {
            City = GetRandomCity(),
            PostCode = GetRandomPostCode()
        });
}

people = repository.GetAll().ToList();
addresses = repository.GetAllAddresses().ToList();
Console.WriteLine($"After Insert People: {people.Count}");
Console.WriteLine($"After Insert Addresses: {addresses.Count}");

List<Person> peopleWithAddres = repository.GetPeopleWithAddresses();

repository.Dispose();

static int GetRandomAge()
    => new Random().Next(15, 45);
static string GetRandomCity()
{
    Random random = new();
    Array values = Enum.GetValues(typeof(Cities));
    int randomIndex = random.Next(values.Length);
    return ((Cities)values.GetValue(randomIndex)).ToString();
}
static int GetRandomPostCode()
      => new Random().Next(1500, 4500);

enum Cities
{
    Varna,
    Burgas,
    Provdiv,
    Sofia,
    Ruse,
    Shumen,
    Sandanski
}