using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions.Sql
{
    class SqlClause
    {
        internal string Name { get; set; }
        internal string InputText { get; set; }
        public string Sql
        {
            get { return this.ToString(); }
        }
        public static SqlClause Parse(string name, string inputText)
        {
            string cleanText = inputText.Replace("\r\n", "").Trim();
            return new SqlClause { Name = name, InputText = cleanText };
        }
        public override string ToString()
        {
            return string.Format("{0} {1}", Name, InputText);
        }
    }
}