using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

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
}
