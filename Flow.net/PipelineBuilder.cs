using Flow.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Flow
{
    public class PipelineBuilder
    {
        private readonly ILogger _logger;
        public PipelineBuilder(ILogger logger)
        {
            _logger = logger;
        }

        public static IPipelineAction CreateAction<T>()
            where T : IPipelineAction, new()
        {
            return new T();
        }

        public static IPipelineAction CreateAction<T>(Action<T> body)
            where T : IPipelineAction, new()
        {
            var action = (T)CreateAction<T>();
            body(action);
            return action;
        }

        /// <summary>
        /// Create an empty pipeline, ready for actions to be added via fluent helpers.
        /// </summary>
        public static IPipeline CreatePipeline()
            => new Pipeline { Actions = Array.Empty<IPipelineAction>() };

        /// <summary>
        /// Create a pipeline from the given actions (sequential).
        /// Accepts null or empty — both produce an empty pipeline (valid no-op).
        /// </summary>
        public static IPipeline CreatePipeline(params IPipelineAction[] actions)
            => new Pipeline { Actions = actions ?? Array.Empty<IPipelineAction>() };

        /// <summary>
        /// Create a pipeline configured by a callback.
        /// Prefer the <c>AddAction&lt;T&gt;</c> extension methods inside <paramref name="body"/>
        /// over direct <c>Actions</c> assignment. If the callback leaves <c>Actions</c> null
        /// (e.g. by reassigning to null), the factory normalizes it to an empty array so
        /// returned pipelines always satisfy the non-null-<c>Actions</c> invariant.
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="body"/> is null.</exception>
        public static IPipeline CreatePipeline(Action<IPipeline> body)
        {
            if (body == null) throw new ArgumentNullException(nameof(body));
            var pipeline = CreatePipeline();
            body(pipeline);
            if (pipeline.Actions == null)
                pipeline.Actions = Array.Empty<IPipelineAction>();
            return pipeline;
        }

        /// <summary>
        /// Create a pipeline containing a single action of type T, configured by body.
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="body"/> is null.</exception>
        public static IPipeline CreatePipeline<T>(Action<T> body)
            where T : IPipelineAction, new()
        {
            if (body == null) throw new ArgumentNullException(nameof(body));
            return CreatePipeline(CreateAction(body));
        }

        public IPipelineBuilder StartWith<T>()
            where T : IPipelineAction, new()
            => StartWith<T>(_ => { });

        public IPipelineBuilder StartWith<T>(Action<T> body)
            where T : IPipelineAction, new()
        { return new Builder(_logger, new IPipelineAction[] { CreateAction(body) }); }

        class Builder : IPipelineBuilder
        {
            private readonly ILogger _logger;

            private readonly List<IPipelineAction> _pipeline = new List<IPipelineAction>();

            public Builder(ILogger logger, IEnumerable<IPipelineAction> pipeline)
            {
                _logger = logger;
                _pipeline.AddRange(pipeline);
            }

            public IPipelineAction CreateAction<T>(Action<T> body)
                where T : IPipelineAction, new()
                => PipelineBuilder.CreateAction(body);
            public IPipelineAction CreateAction<T>()
                where T : IPipelineAction, new()
                => PipelineBuilder.CreateAction<T>();

            public async Task<IValueSource> ExecuteAsync(CancellationToken cancellationToken = default)
            { return await Create().ExecuteAsync(new ExecutionContext(_logger, cancellationToken)); }

            public IPipelineBuilder ContinueWith<T>()
                where T : IPipelineAction, new()
            {
                return new Builder(_logger, _pipeline.Append(CreateAction<T>()));
            }

            public IPipelineBuilder ContinueWith<T>(Action<T> body)
                where T : IPipelineAction, new()
            {
                return new Builder(_logger, _pipeline.Append(CreateAction(body)));
            }

            public IPipeline Create() { return PipelineBuilder.CreatePipeline(_pipeline.ToArray()); }

            public async Task<T> ExecuteAsync<T>(CancellationToken cancellationToken = default) where T : IValueSource
                => (T)await ExecuteAsync(cancellationToken);
        }
    }
}
