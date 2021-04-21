using System;
using System.Collections.Generic;
using System.Linq;

namespace Flow.Data
{
    public interface IDbAction : IPipelineAction
    {
        string ConnectionString { get; set; }

        string Query { get; set; }

        int? CommandTimeout { get; set; }

        IEnumerable<IQueryParameterBuilder> Parameters { get; set; }
    }
}
