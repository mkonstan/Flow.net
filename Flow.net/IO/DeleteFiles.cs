using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Flow.IO
{
    public class DeleteFiles : PipelineAction
    {
        public DeleteFiles()
        {
            SetTypeHandler<ValueCollection<string>>(async (context, input) => await Handler(this, context, input));
            SetTypeHandler<FilePath>(async (context, input) => await Handler(this, context, input));
            SetTypeHandler<PayloadCollection>(async (context, input) => await Handler(this, context, input));
        }

        private static async Task<IValue> Handler(DeleteFiles that, IExecutionContext context, PayloadCollection paths)
        { return await Handler(that, context, paths.Cast<FilePath>().ToArray()); }

        private static async Task<IValue> Handler(DeleteFiles that, IExecutionContext context, params FilePath[] paths)
        { return await Handler(that, context, paths.Select(p => p.Path).ToArray()); }

        private static async Task<IValue> Handler(DeleteFiles that, IExecutionContext context, IEnumerable<string> paths)
        {
            return await Task.Run(
                () =>
                {
                    Parallel.ForEach(paths, file => File.Delete(file));
                    return NullResult.Instance;
                });
        }
    }
}
