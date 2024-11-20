using System.Net;

namespace SqliteWithDapper.Models
{
    public class Person
    {

        public Person()
        {
            Addresses = [];
        }

        public int Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }

        public List<Address> Addresses { get; set; }
    }
}
