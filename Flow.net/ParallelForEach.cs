using System;
using System.Collections.Generic;
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

            var elements = input.ToArray();
            var actions = that.Actions.ToArray();

            try
            {
                var results = await ParallelExecution.RunWithConcurrencyCapAsync(
                    elements,
                    that.MaxDegreeOfParallelism,
                    async element =>
                    {
                        var e = element;
                        foreach (var action in actions)
                        {
                            e = await action.ExecuteAsync(context.New(e));
                        }
                        return e;
                    },
                    context.CancellationToken);
                return new PayloadCollection(results);
            }
            catch (AggregateException agg)
            {
                throw new ParallelForEachException(
                    $"One or more elements failed in ParallelForEach ({agg.InnerExceptions.Count} failure(s)).",
                    agg);
            }
        }
    }
}
