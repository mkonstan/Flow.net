using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Flow
{
    /// <summary>
    /// Iterates over elements in a PayloadCollection in parallel, running Actions sequentially per element.
    /// Action instances are shared across parallel elements — actions must be stateless/thread-safe.
    /// </summary>
    public class ParallelForEach : PipelineAction, IPipeline
    {
        public static readonly int ProcessorCount = Environment.ProcessorCount;

        public ParallelForEach()
        { SetTypeHandler<PayloadCollection>(async (context, input) => await Handler(this, context, input)); }

        public int MaxDegreeOfParallelism { get; set; } = ProcessorCount;

        public IEnumerable<IPipelineAction> Actions { get; set; }

        private static async Task<IValueSource> Handler(
            ParallelForEach that,
            IExecutionContext context,
            PayloadCollection input)
        {
            if (that.Actions == null)
                throw new ActionConfigurationException(that.GetType().Name, "Actions must be set before execution.");

            int maxDop = that.MaxDegreeOfParallelism <= 0 ? ProcessorCount : that.MaxDegreeOfParallelism;
            var elements = input.ToArray();
            var actions = that.Actions.ToArray();
            var results = new IValueSource[elements.Length];

            for (int i = 0; i < elements.Length; i += maxDop)
            {
                var batch = Enumerable.Range(i, Math.Min(maxDop, elements.Length - i));
                var whenAll = Task.WhenAll(batch.Select(async idx =>
                {
                    var e = elements[idx];
                    foreach (var action in actions)
                    {
                        e = await action.ExecuteAsync(context.New(e));
                    }
                    results[idx] = e;
                }));
                try
                {
                    await whenAll;
                }
                catch
                {
                    throw new ParallelForEachException(
                        $"One or more elements failed in ParallelForEach ({whenAll.Exception.InnerExceptions.Count} failure(s)).",
                        whenAll.Exception);
                }
            }

            return new PayloadCollection(results);
        }
    }
}
