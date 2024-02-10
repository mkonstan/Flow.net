using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Flow
{
    public class ForEach : PipelineAction, IPipeline
    {
        public ForEach()
        { SetTypeHandler<PayloadCollection>(async (context, input) => await Handler(this, context, input)); }

        public IEnumerable<IPipelineAction> Actions { get; set; }

        private static async Task<IValue> Handler(ForEach that, IExecutionContext context, PayloadCollection input)
        {
            var results = new List<IValue>();
            foreach (var element in input)
            {
                //var newContext = context.New();
                var e = element;
                foreach (var action in that.Actions)
                {
                    e = await action.ExecuteAsync(context.New(e));
                }
                results.Add(e);
            }
            return new PayloadCollection(results);
        }
    }
}
