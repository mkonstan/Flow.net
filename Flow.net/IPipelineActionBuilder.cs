using System;
using System.Linq;

namespace Flow
{
    public interface IPipelineActionBuilder
    {
        IPipelineAction CreateAction<T>(Action<T> body) where T : IPipelineAction, new();
    }
}
