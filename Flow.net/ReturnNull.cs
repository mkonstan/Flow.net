using System.Threading.Tasks;

namespace Flow
{
    public sealed class ReturnNull : PipelineAction
    {
        public ReturnNull()
        {
            SetTypeHandler<IValueSource>(
                (context, input) => Task.FromResult<IValueSource>(NullResult.Instance));
        }
    }
}
