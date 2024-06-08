using System;

namespace N.EntityFrameworkCore.Extensions.Test.Data
{
    public class TphCustomer : TphPerson
    {
        public string Email { get; set; }
        public string Phone { get; set; }
        public DateTime AddedDate { get; set; }
    }
}