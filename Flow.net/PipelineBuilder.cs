using Flow.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
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

            public async Task<IPayload> ExecuteAsync()
            { return await Create().ExecuteAsync(new ExecutionContext(_logger, new System.Threading.CancellationTokenSource())); }

            public IPipelineBuilder ContinueWith<T>()
                where T : IPipelineAction, new()
            {
                _pipeline.Add(CreateAction<T>());
                return new Builder(_logger, _pipeline);
            }

            public IPipelineBuilder ContinueWith<T>(Action<T> body)
                where T : IPipelineAction, new()
            {
                _pipeline.Add(CreateAction(body));
                return new Builder(_logger, _pipeline);
            }

            public IPipeline Create() { return new Pipeline { Actions = _pipeline }; }

            public async Task<T> ExecuteAsync<T>() where T : IPayload
                => (T)await ExecuteAsync();
        }
    }
}
