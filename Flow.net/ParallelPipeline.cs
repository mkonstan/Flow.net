using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Flow
{
    /// <summary>
    /// Executes Actions in parallel batches. Each action receives the same input.
    /// Action instances are invoked concurrently — actions must be stateless/thread-safe.
    /// </summary>
    public class ParallelPipeline : PipelineAction, IPipeline
    {
        public static readonly int ProcessorCount = Environment.ProcessorCount;

        public int MaxDegreeOfParallelism { get; set; } = ProcessorCount;
        public IEnumerable<IPipelineAction> Actions { get; set; }

        protected override async Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
        {
            if (Actions == null)
                throw new ActionConfigurationException(GetType().Name, "Actions must be set before execution.");

            int maxDegreeOfParallelism = MaxDegreeOfParallelism <= 0 ? ProcessorCount : MaxDegreeOfParallelism;
            var actions = Actions.ToArray();
            var results = new List<IValueSource>();

            // Process in batches of maxDegreeOfParallelism
            for (int i = 0; i < actions.Length; i += maxDegreeOfParallelism)
            {
                var batch = actions.Skip(i).Take(maxDegreeOfParallelism);
                var whenAll = Task.WhenAll(batch.Select(a => a.ExecuteAsync(context.New(input))));
                try
                {
                    results.AddRange(await whenAll);
                }
                catch
                {
                    throw new ParallelPipelineException(
                        $"One or more actions failed in ParallelPipeline ({whenAll.Exception.InnerExceptions.Count} failure(s)).",
                        whenAll.Exception);
                }
            }

            return new PayloadCollection(results);
        }
    }
}
