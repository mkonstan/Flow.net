using System;
using System.Data;
using System.Linq;

namespace Flow.Data
{
    public class QueryParameterBuilder : IQueryParameterBuilder
    {
        public string Name { get; set; }

        public DbType? Type { get; set; }

        public int? Size { get; set; }

        public object Value { get; set; }

        public IQueryParameter Create(IExecutionContext context, IPayload input, IPipelineAction action)
        { return new QueryParameter { Name = Name, Type = Type, Size = Size, Value = Value }; }
    }
}
