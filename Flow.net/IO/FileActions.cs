using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel;
using System.Collections.Concurrent;

namespace Flow.IO
{


    public class GetFiles : PipelineAction
    {
        public string DirectoryPath { get; set; }
        public string SearchPattern { get; set; } = "*";
        public SearchOption SearchOption { get; set; } = SearchOption.TopDirectoryOnly;

        protected override async Task<IPayload> DefaultHandlerAsync(IExecutionContext context, IPayload input)
        {
            var directory = Format(DirectoryPath, context, NullResult.Instance, this);
            var searchPattern = Format(SearchPattern, context, NullResult.Instance, this);
            var files = Directory.GetFiles(directory, searchPattern, SearchOption);
            return await Task.FromResult(new FilePathCollection(files));
        }
    }

    public class DeleteFiles : PipelineAction
    {
        public DeleteFiles()
        {
            SetTypeHandler<ValueCollection<string>>(async (context, input) => await Handler(this, context, input));
            SetTypeHandler<FilePath>(async (context, input) => await Handler(this, context, input));
            SetTypeHandler<PayloadCollection>(async (context, input) => await Handler(this, context, input));
        }

        private static async Task<IPayload> Handler(DeleteFiles that, IExecutionContext context, PayloadCollection paths)
        { return await Handler(that, context, paths.Cast<FilePath>().ToArray()); }

        private static async Task<IPayload> Handler(DeleteFiles that, IExecutionContext context, params FilePath[] paths)
        { return await Handler(that, context, paths.Select(p => p.Path).ToArray()); }

        private static async Task<IPayload> Handler(DeleteFiles that, IExecutionContext context, IEnumerable<string> paths)
        {
            return await Task.Run(
                () =>
                {
                    Parallel.ForEach(paths, file => File.Delete(file));
                    return NullResult.Instance;
                });
        }
    }

    public class CopyFiles : PipelineAction
    {
        public string DirectoryPath { get; set; }
        public CopyFiles()
        {
            SetTypeHandler<ValueCollection<string>>(async (context, input) => await Handler(this, context, input));
            SetTypeHandler<FilePath>(async (context, input) => await Handler(this, context, input));
            SetTypeHandler<PayloadCollection>(async (context, input) => await Handler(this, context, input));
        }

        private static async Task<IPayload> Handler(CopyFiles that, IExecutionContext context, PayloadCollection paths)
        { return await Handler(that, context, paths.Cast<FilePath>().ToArray()); }

        private static async Task<IPayload> Handler(CopyFiles that, IExecutionContext context, params FilePath[] paths)
        { return await Handler(that, context, paths.Select(p => p.Path).ToArray()); }

        private static async Task<IPayload> Handler(CopyFiles that, IExecutionContext context, IEnumerable<string> paths)
        {
            return await Task.Run(
                () =>
                {
                    ConcurrentQueue<string> newPaths = new ConcurrentQueue<string>();
                    Parallel.ForEach(paths, file => {
                        string newFile = Path.Combine(that.DirectoryPath, Path.GetFileName(file));
                        File.Copy(file, newFile);
                        newPaths.Enqueue(newFile);
                    });
                    return new FilePathCollection(newPaths);
                });
        }
    }

    public class MoveFiles: PipelineAction
    {
        public string DirectoryPath { get; set; }
        public MoveFiles()
        {
            // Why use value collection and file path?
            // benefits of having a FilePathCollection? only I can think of is allowing stronger config valdiations, would only be useful for a flow.gui interface, but that's a worthy cause
            SetTypeHandler<ValueCollection<string>>(async (context, input) => await Handler(this, context, input));
            SetTypeHandler<FilePath>(async (context, input) => await Handler(this, context, input));
            SetTypeHandler<PayloadCollection>(async (context, input) => await Handler(this, context, input));
        }

        private static async Task<IPayload> Handler(MoveFiles that, IExecutionContext context, PayloadCollection paths)
        { return await Handler(that, context, paths.Cast<FilePath>().ToArray()); }

        private static async Task<IPayload> Handler(MoveFiles that, IExecutionContext context, params FilePath[] paths)
        { return await Handler(that, context, paths.Select(p => p.Path).ToArray()); }

        private static async Task<IPayload> Handler(MoveFiles that, IExecutionContext context, IEnumerable<string> paths)
        {
            return await Task.Run(
                () =>
                {
                    ConcurrentQueue<string> newPaths = new ConcurrentQueue<string>();
                    Parallel.ForEach(paths, file => {
                        string newFile = Path.Combine(that.DirectoryPath, Path.GetFileName(file));
                        File.Move(file, newFile);
                        newPaths.Enqueue(newFile);
                    });
                    return new FilePathCollection(newPaths);
                });
        }
    }

}
