using System;

namespace Flow
{
    public static class ParallelPipelineHelpers
    {
        /// <summary>
        /// Add a parallel branch by configuring a new pipeline via callback. Chainable.
        /// Delegates to <c>PipelineBuilder.CreatePipeline(Action&lt;IPipeline&gt;)</c> so the
        /// post-callback Actions normalization applies uniformly.
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="body"/> is null.</exception>
        public static ParallelPipeline AddPipeline(
            this ParallelPipeline source,
            Action<IPipeline> body)
        {
            if (body == null) throw new ArgumentNullException(nameof(body));
            return source.AddPipeline(PipelineBuilder.CreatePipeline(body));
        }

        /// <summary>
        /// Add a parallel branch consisting of a single action of type T, configured by body. Chainable.
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="body"/> is null.</exception>
        public static ParallelPipeline AddPipeline<T>(
            this ParallelPipeline source,
            Action<T> body)
            where T : IPipelineAction, new()
        {
            if (body == null) throw new ArgumentNullException(nameof(body));
            return source.AddPipeline(PipelineBuilder.CreatePipeline(body));
        }
    }
}
