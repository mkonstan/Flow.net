using System;
using System.Data;
using System.Linq;

namespace Flow.Data
{
    public class QueryParameter : IQueryParameter
    {
        public string Name { get; set; }

        public object Value { get; set; }

        public DbType? Type { get; set; }

        public ParameterDirection? ParameterDirection { get; set; }

        public int? Size { get; set; }
    }
}
