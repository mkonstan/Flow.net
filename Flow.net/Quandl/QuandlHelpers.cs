using Flurl;
using Flurl.Http;
using Ionic.Zip;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Flow.Quandl;

namespace Flow.Quandl
{
    public static class QuandlHelpers
    {
        public static readonly Action<string> DefaultCallback = (_) =>
        {
        };
        private const int MaxDownloadAttemps = 10;

        public static IEnumerable<string> Unzip(this string zipFile, string targerDirectory)
        {
            var files = new List<string>();
            using (var zreader = ZipFile.Read(zipFile))
            {
                foreach (var file in zreader.Entries.Where(et => !et.IsDirectory))
                {
                    file.Extract(targerDirectory, ExtractExistingFileAction.OverwriteSilently);
                    files.Add(Path.Combine(targerDirectory, file.FileName));
                }
            }
            return files;
        }

        public static async Task<string> DownloadFileAsync(this Url url, string targerDirectory) => await DownloadFileAsync(
            url,
            targerDirectory,
            DefaultCallback,
            MaxDownloadAttemps);

        public static async Task<string> DownloadFileAsync(
            this Url url,
            string targerDirectory,
            Action<string> callback,
            int maxAttemps = MaxDownloadAttemps)
        {
            var attempCounter = 0;
            if (Directory.Exists(targerDirectory))
                Directory.CreateDirectory(targerDirectory);
            while (attempCounter < maxAttemps)
            {
                try
                {
                    var result = await url.GetJsonAsync<Rootobject>();

                    callback($"data {result.Download.File.Status}");
                    if (!result.Download.File.Status.Equals("fresh", StringComparison.OrdinalIgnoreCase))
                    {
                        await Task.Delay(TimeSpan.FromSeconds(10));
                        continue;
                    }
                    return await result.Download.File.Link.DownloadFileAsync(targerDirectory);
                }
                catch (Exception e)
                {
                    attempCounter += 1;
                    callback(
                        $"attempt {attempCounter} of {maxAttemps} failed. Waiting 10sec before tryin again. ERROR [{e.Message}]");
                }
            }
            throw new Exception($"download failed! [URL: ${url}]");
        }

        private class Rootobject
        {
            [JsonProperty("datatable_bulk_download")]
            public DownloadInfo Download { get; set; }
        }

        private class DownloadInfo
        {
            public DownloadFileInfo File { get; set; }
        }

        private class DownloadFileInfo
        {
            public string Link { get; set; }

            public string Status { get; set; }

            [JsonProperty("data_snapshot_time")]
            public string Date { get; set; }
        }
    }
}
