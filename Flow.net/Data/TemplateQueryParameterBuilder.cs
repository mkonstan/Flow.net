using System;
using System.Data;
using System.Linq;

namespace Flow.Data
{
    public class TemplateQueryParameterBuilder : IQueryParameterBuilder
    {
        private static readonly SmartFormat.SmartFormatter Formatter = SmartFormat.Smart.CreateDefaultSmartFormat();

        public string Name { get; set; }

        public DbType? Type { get; set; }

        public int? Size { get; set; }

        public string Template { get; set; }

        public IQueryParameter Create(IExecutionContext context, IPayload input, IPipelineAction action)
        {
            var value = Formatter.Format(Template, (action, input, context));
            return new QueryParameter { Name = Name, Type = Type, Size = Size, Value = value };
        }
    }
}
