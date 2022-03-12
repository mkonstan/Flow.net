using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel;
using System.Collections.Concurrent;

namespace Flow.IO
{
    //public class MoveFiles : PipelineAction
    //{
    //    public string Directory { get; set; }
    //    public MoveFiles()
    //    {
    //        // Why use value collection and file path?
    //        // benefits of having a FilePathCollection? only I can think of is allowing stronger config valdiations, would only be useful for a flow.gui interface, but that's a worthy cause
    //        SetTypeHandler<ValueCollection<string>>(async (context, input) => await Handler(this, context, input));
    //        SetTypeHandler<FilePath>(async (context, input) => await Handler(this, context, input));
    //        SetTypeHandler<PayloadCollection>(async (context, input) => await Handler(this, context, input));
    //    }

    //    private static async Task<IPayload> Handler(MoveFiles that, IExecutionContext context, PayloadCollection paths)
    //    { return await Handler(that, context, paths.Cast<FilePath>().ToArray()); }

    //    private static async Task<IPayload> Handler(MoveFiles that, IExecutionContext context, params FilePath[] paths)
    //    { return await Handler(that, context, paths.Select(p => p.Path).ToArray()); }

    //    private static async Task<IPayload> Handler(MoveFiles that, IExecutionContext context, IEnumerable<string> paths)
    //    {
    //        return await Task.Run(
    //            () =>
    //            {
    //                ConcurrentQueue<string> newPaths = new ConcurrentQueue<string>();
    //                Parallel.ForEach(paths, file => {
    //                    string newFile = Path.Combine(that.Directory, Path.GetFileNameWithoutExtension(file));
    //                    File.Move(file, newFile);
    //                    newPaths.Enqueue(newFile);
    //                });
    //                return new FilePathCollection(newPaths);
    //            });
    //    }
    //}
}
