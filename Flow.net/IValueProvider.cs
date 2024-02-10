namespace Flow
{
	public interface IValueProvider
    {
        IValue Get(IExecutionContext context, IPipelineAction action);
        T Get<T>(IExecutionContext context, IPipelineAction action) where T : IValue;
    }
}
