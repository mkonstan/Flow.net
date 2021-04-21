using System;
using System.Data;
using System.Linq;

namespace Flow.Data
{
    public interface IQueryParameter
    {
        string Name { get; set; }

        object Value { get; set; }

        DbType? Type { get; set; }

        ParameterDirection? ParameterDirection { get; set; }

        int? Size { get; set; }
    }
}
