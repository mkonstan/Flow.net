using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Flow
{
    /// <summary>
    /// Executes Pipelines in parallel with a semaphore-gated concurrency cap. Each pipeline receives the same input.
    /// Pipeline instances are invoked concurrently — pipelines (and their actions) must be stateless/thread-safe.
    /// </summary>
    public class ParallelPipeline : PipelineAction
    {
        public static readonly int ProcessorCount = Environment.ProcessorCount;

        private readonly List<IPipeline> _pipelines = new List<IPipeline>();

        public int MaxDegreeOfParallelism { get; set; } = ProcessorCount;

        /// <summary>
        /// Pipelines to run in parallel. Each receives the same input.
        /// Setter replaces the list contents (does not merge).
        /// </summary>
        public IEnumerable<IPipeline> Pipelines
        {
            get => _pipelines;
            set
            {
                _pipelines.Clear();
                if (value != null) _pipelines.AddRange(value);
            }
        }

        /// <summary>
        /// Add a pre-built pipeline as a parallel branch. Chainable.
        /// </summary>
        public ParallelPipeline AddPipeline(IPipeline pipeline)
        {
            if (pipeline == null) throw new ArgumentNullException(nameof(pipeline));
            _pipelines.Add(pipeline);
            return this;
        }

        /// <summary>
        /// Add a new single-step pipeline wrapping the given actions (sequential within the branch).
        /// Common case: one action per branch — <c>AddPipeline(new SendMetric())</c>.
        /// Multi-action case: sequential sub-pipeline — <c>AddPipeline(new LogAudit(), new Notify())</c>.
        /// Chainable.
        /// </summary>
        public ParallelPipeline AddPipeline(params IPipelineAction[] actions)
        {
            if (actions == null || actions.Length == 0)
                throw new ArgumentException("At least one action is required.", nameof(actions));
            _pipelines.Add(new Pipeline { Actions = actions });
            return this;
        }

        protected override async Task<IValueSource> DefaultHandlerAsync(IExecutionContext context, IValueSource input)
        {
            if (_pipelines.Count == 0)
                throw new ActionConfigurationException(GetType().Name,
                    "Pipelines must be set (or added via AddPipeline) before execution.");

            try
            {
                var results = await ParallelExecution.RunWithConcurrencyCapAsync(
                    _pipelines,
                    MaxDegreeOfParallelism,
                    p => p.ExecuteAsync(context.New(input)),
                    context.CancellationToken);
                return new PayloadCollection(results);
            }
            catch (AggregateException agg)
            {
                throw new ParallelPipelineException(
                    $"One or more pipelines failed in ParallelPipeline ({agg.InnerExceptions.Count} failure(s)).",
                    agg);
            }
        }
    }
}
