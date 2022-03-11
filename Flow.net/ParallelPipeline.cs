using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Flow
{
    public class ParallelPipeline : PipelineAction, IPipeline
    {
        public static readonly int ProcessorCount = Environment.ProcessorCount;

        public int MaxDegreeOfParallelism { get; set; } = ProcessorCount;
        public IEnumerable<IPipelineAction> Actions { get; set; }

        protected override async Task<IPayload> DefaultHandlerAsync(IExecutionContext context, IPayload input)
        {
            int maxdegreeOfParallelism = MaxDegreeOfParallelism <= 0 ? ProcessorCount : MaxDegreeOfParallelism;
            var pipelinePartitios = Actions
                .Select((action, index) => new { action, index })
                .GroupBy(v => v.index);
            var results = new List<IPayload>();
            foreach (var pipeline in pipelinePartitios)
            {
                var result = await Task.WhenAll(pipeline.Select(p => p.action.ExecuteAsync(context.New(input))).ToArray());
                results.AddRange(result);
            }
            return new PayloadCollection(results);
        }
    }
}
