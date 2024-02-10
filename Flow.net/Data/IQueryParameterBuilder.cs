using System;
using System.Data;
using System.Linq;

namespace Flow.Data
{
    public interface IQueryParameterBuilder
    {
        string Name { get; set; }

        DbType? Type { get; set; }

        int? Size { get; set; }

        IQueryParameter Create(IExecutionContext context, IValue input, IPipelineAction action);
    }
}
