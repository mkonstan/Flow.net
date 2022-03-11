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

        public static IPipelineAction CreateAction<T>(Action<T> body)
            where T : IPipelineAction, new()
        {
            var action = new T();
            body(action);
            return action;
        }

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
            {
                var action = new T();
                body(action);
                return action;
            }

            public async Task<IPayload> ExecuteAsync()
            { return await Create().ExecuteAsync(new ExecutionContext(_logger), NullResult.Instance); }

            public IPipelineBuilder ContinueWith<T>(Action<T> body)
                where T : IPipelineAction, new()
            {
                _pipeline.Add(CreateAction<T>(body));
                return new Builder(_logger, _pipeline);
            }

            public IPipeline Create() { return new Pipeline { Actions = _pipeline }; }

            public async Task<T> ExecuteAsync<T>() where T : IPayload
                => (T)await ExecuteAsync();
        }
    }
}
