using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
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

        private static async Task<IValueSource> Handler(UnzipFile that, IExecutionContext context, PayloadCollection paths)
        { return await Handler(that, context, paths.Cast<FilePath>().ToArray()); }

        private static async Task<IValueSource> Handler(UnzipFile that, IExecutionContext context, params FilePath[] paths)
        { return await Handler(that, context, paths.Select(p => p.Path).ToArray()); }

        private static async Task<IValueSource> Handler(UnzipFile that, IExecutionContext context, IEnumerable<string> paths)
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
                                using (var archive = ZipFile.OpenRead(path))
                                {
                                    foreach (var entry in archive.Entries.Where(e => !string.IsNullOrEmpty(e.Name)))
                                    {
                                        var destinationPath = Path.Combine(workingDirectory, entry.FullName);
                                        var destinationDir = Path.GetDirectoryName(destinationPath);
                                        if (!Directory.Exists(destinationDir))
                                            Directory.CreateDirectory(destinationDir);
                                        entry.ExtractToFile(destinationPath, overwrite: true);
                                        files.Add(destinationPath);
                                    }
                                }
                                return files;
                            });
                    return new FilePathCollection(result);
                });
        }
    }
}
