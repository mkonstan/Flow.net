using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Flow
{
    public class SetScopedResponse : PipelineAction
    {
        public string Name { get; set; }
        protected override async Task<IPayload> DefaultHandlerAsync(IExecutionContext context, IPayload input)
        {
            context[Name] = input;
            return await Task.FromResult(input);
        }
    }

    public class SetScopedVariables : PipelineAction
    {
        public IDictionary<string, object> Variables = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        public void AddStateVariable(string name, object value) => Variables[name] = value;
        protected override async Task<IPayload> DefaultHandlerAsync(IExecutionContext context, IPayload input)
        {
            foreach (var element in Variables)
            {
                context[element.Key] = element.Value;
            }
            return await Task.FromResult(input);
        }
    }
}
