using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Flow
{
    public class ParallelForEach : PipelineAction, IPipeline
    {
        public static readonly int ProcessorCount = Environment.ProcessorCount;

        public ParallelForEach()
        { SetTypeHandler<PayloadCollection>(async (context, input) => await Handler(this, context, input)); }

        public int MaxDegreeOfParallelism { get; set; } = ProcessorCount;

        public IEnumerable<IPipelineAction> Actions { get; set; }

        private static async Task<IPayload> Handler(
            ParallelForEach that,
            IExecutionContext context,
            PayloadCollection input)
        {
            var result = new List<IPayload>(input.Count());
            var actionGroups = that.Actions.Select((action, index) => (action, index)).GroupBy(a => ((a.index+1) % that.MaxDegreeOfParallelism) == 0);
            foreach (var actionGroup in actionGroups)
            {
                var actions = actionGroup.Select(a => a.action);
                result.AddRange(
                    await Task.WhenAll(
                        input.SelectMany(p => actions.Select(a => a.ExecuteAsync(context.New(input)))).ToArray()));
            }
            return new PayloadCollection(result);
        }
    }
}
