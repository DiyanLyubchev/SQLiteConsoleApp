namespace SqliteWithDapper.Models
{
    public class Address
    {
        public int Id { get; set; }
        public string City { get; set; }
        public int PostCode { get; set; }
        public int PersonId { get; set; }
    }
}
