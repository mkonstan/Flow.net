using Ionic.Zip;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Flow.IO
{
    public class UnzipFile : PipelineAction
    {
        public UnzipFile()
        {
            SetTypeHandler<ValueCollection<string>>(async (context, input) => await Handler(this, context, input));
            SetTypeHandler<FilePath>(async (context, input) => await Handler(this, context, input));
            SetTypeHandler<PayloadCollection>(async (context, input) => await Handler(this, context, input));
        }

        public string WorkingDirectory { get; set; }

        private static async Task<IPayload> Handler(UnzipFile that, IExecutionContext context, PayloadCollection paths)
        { return await Handler(that, context, paths.Cast<FilePath>().ToArray()); }

        private static async Task<IPayload> Handler(UnzipFile that, IExecutionContext context, params FilePath[] paths)
        { return await Handler(that, context, paths.Select(p => p.Path).ToArray()); }

        private static async Task<IPayload> Handler(UnzipFile that, IExecutionContext context, IEnumerable<string> paths)
        {
            return await Task.Run(
                () =>
                {
                    var workingDirectory = Format(that.WorkingDirectory, context, NullResult.Instance, that);
                    var result = paths.AsParallel()
                        .SelectMany(
                            path =>
                            {
                                var files = new List<string>();
                                using (var zreader = ZipFile.Read(path))
                                {
                                    foreach (var file in zreader.Entries.Where(et => !et.IsDirectory))
                                    {
                                        file.Extract(workingDirectory, ExtractExistingFileAction.OverwriteSilently);
                                        files.Add(Path.Combine(workingDirectory, file.FileName));
                                    }
                                }
                                return files;
                            });
                    return new FilePathCollection(result);
                });
        }
    }
}
