using Flow.Data;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Flow
{
    public static class PipelineHelpers
    {
        public static IPipeline AddAction(this IPipeline source, IPipelineAction action)
        {
            var actions = new List<IPipelineAction>(source.Actions ?? new IPipelineAction[] { });
            actions.Add(action);
            source.Actions = actions;
            return source;
        }

        public static IPipeline AddAction<T>(this IPipeline source, Action<T> body)
            where T : IPipelineAction, new()
        { return source.AddAction(PipelineBuilder.CreateAction<T>(body)); }

        public static IDbAction AddParameter(this IDbAction source, IQueryParameterBuilder parameter)
        {
            var parameters = new List<IQueryParameterBuilder>(source.Parameters ?? new IQueryParameterBuilder[] { });
            parameters.Add(parameter);
            source.Parameters = parameters;
            return source;
        }
    }
}
