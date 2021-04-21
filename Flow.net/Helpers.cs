using Ionic.Zip;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace Flow
{
    static class LinqHelpers
    {
        public static HashSet<T> ToHashSet<T>(this IEnumerable<T> source, IEqualityComparer<T> comparer)
            => new HashSet<T>(source, comparer);
    }

    static class JsonHelpers
    {
        public static string Serialize<T>(this T source) { return JsonConvert.SerializeObject(source); }

        public static T Deserialize<T>(this string source) { return JsonConvert.DeserializeObject<T>(source); }

    }

    //public static class Helpers
    //{
    //    //public static Flurl.Url SetQueryParam(this Flurl.Url url, (string name, object value) arg)
    //    //{ return url.SetQueryParams(Flurl.NullValueHandling.Remove, new[] { arg }); }

    //    //public static Flurl.Url SetQueryParams(this Flurl.Url url, params (string name, object value)[] args)
    //    //{ return url.SetQueryParams(Flurl.NullValueHandling.Remove, args); }

    //    //public static Flurl.Url SetQueryParams(
    //    //    this Flurl.Url url,
    //    //    Flurl.NullValueHandling nullValueHandling,
    //    //    params (string name, object value)[] args)
    //    //{
    //    //    foreach (var arg in args)
    //    //    {
    //    //        url = url.SetQueryParam(arg.name, arg.value, nullValueHandling);
    //    //    }
    //    //    return url;
    //    //}

    //    //private class BlockingCollectionPartitioner<T> : Partitioner<T>
    //    //{
    //    //    private BlockingCollection<T> _collection;

    //    //    internal BlockingCollectionPartitioner(BlockingCollection<T> collection)
    //    //    {
    //    //        if (collection == null)
    //    //            throw new ArgumentNullException("collection");
    //    //        _collection = collection;
    //    //    }

    //    //    public override bool SupportsDynamicPartitions { get; } = true;

    //    //    public override IList<IEnumerator<T>> GetPartitions(int partitionCount)
    //    //    {
    //    //        if (partitionCount < 1)
    //    //            throw new ArgumentOutOfRangeException("partitionCount");
    //    //        var dynamicPartitioner = GetDynamicPartitions();
    //    //        return Enumerable.Range(0, partitionCount).Select(_ => dynamicPartitioner.GetEnumerator()).ToArray();
    //    //    }

    //    //    public override IEnumerable<T> GetDynamicPartitions() { return _collection.GetConsumingEnumerable(); }
    //    //}

    //    //public static Partitioner<T> GetConsumingPartitioner<T>(this BlockingCollection<T> collection)
    //    //    => new BlockingCollectionPartitioner<T>(collection);

    //    //public static async Task<HttpResponseMessage> TrySendAsync(
    //    //    this HttpClient client,
    //    //    Func<HttpRequestMessage> requestBuilder,
    //    //    int retryCouter = 5)
    //    //{
    //    //    for (int i = 0; i < retryCouter; i++)
    //    //    {
    //    //        var request = requestBuilder();
    //    //        try
    //    //        {
    //    //            var responce = await client.SendAsync(request);
    //    //            if (!responce.IsSuccessStatusCode)
    //    //            {
    //    //                //responce.ReasonPhrase.Dump("Status Message");
    //    //                continue;
    //    //            }
    //    //            return responce;
    //    //        }
    //    //        catch
    //    //        {
    //    //            //throw new Exception($"Atempt {i} of {retryCouter}: {e.Message}; {request} [{e}]");
    //    //        }
    //    //        Thread.Sleep(System.TimeSpan.FromSeconds(1));
    //    //    }
    //    //    throw new Exception("Unable to download requested information!");
    //    //}

    //    //public static IEnumerable<IEnumerable<T>> Page<T>(this IEnumerable<T> source, int pageSize)
    //    //{
    //    //    var data = new List<T>(pageSize);
    //    //    foreach (var element in source)
    //    //    {
    //    //        data.Add(element);
    //    //        if (data.Count == pageSize)
    //    //        {
    //    //            yield return data.ToArray();
    //    //            data.Clear();
    //    //        }
    //    //    }
    //    //    if (data.Count > 0)
    //    //        yield return data.ToArray();
    //    //}

    //    //public static IEnumerable<string> Lines(this StreamReader reader)
    //    //{
    //    //    while (!reader.EndOfStream)
    //    //    {
    //    //        yield return reader.ReadLine();
    //    //    }
    //    //}

    //    //public static void DownloadFile(this WebClient source, string address, Action<string> body)
    //    //{
    //    //    var tempFile = Path.GetTempFileName();
    //    //    try
    //    //    {
    //    //        source.DownloadFile(address, tempFile);
    //    //        body(tempFile);
    //    //    }
    //    //    finally
    //    //    {
    //    //        if (File.Exists(tempFile))
    //    //            File.Delete(tempFile);
    //    //    }
    //    //}

    //    //public static string ReadAllText(this ZipEntry entry)
    //    //{
    //    //    using (var s = entry.OpenReader())
    //    //    {
    //    //        using (var s1 = new StreamReader(s))
    //    //        {
    //    //            return s1.ReadToEnd();
    //    //        }
    //    //    }
    //    //}

    //    //public static async Task<string> UrlEncodedContent(this IEnumerable<KeyValuePair<string, string>> parameters)
    //    //{
    //    //    using (var o = new FormUrlEncodedContent(parameters))
    //    //    {
    //    //        return await o.ReadAsStringAsync();
    //    //    }
    //    //}

    //    //public static bool TryGetValues(
    //    //    this HttpResponseHeaders source,
    //    //    string name,
    //    //    Func<IEnumerable<string>, bool> body)
    //    //{ return source.TryGetValues(name, out var values) && body(values); }

    //    //public static T? Value<T>(this string source)
    //    //    where T : struct
    //    //{ return (T?)TypeConverters[typeof(T?)](source); }

    //    //public static T Value<T>(this string source, T defaultValue)
    //    //    where T : struct
    //    //{ return (T?)TypeConverters[typeof(T?)](source) ?? defaultValue; }

    //    //internal static IDictionary<Type, Func<string, object>> TypeConverters = GetTypeConverters();

    //    //internal static IDictionary<Type, Func<string, object>> GetTypeConverters()
    //    //{
    //    //    var converters = new Dictionary<Type, Func<string, object>>();
    //    //    {
    //    //        converters[typeof(string)] = value => $"{value}";
    //    //        converters[typeof(int)] = value => int.Parse(value);
    //    //        converters[typeof(uint)] = value => uint.Parse(value);
    //    //        converters[typeof(long)] = value => long.Parse(value);
    //    //        converters[typeof(ulong)] = value => ulong.Parse(value);
    //    //        converters[typeof(float)] = value => float.Parse(value);
    //    //        converters[typeof(double)] = value => double.Parse(value);
    //    //        converters[typeof(decimal)] = value => decimal.Parse(value);
    //    //        converters[typeof(bool)] = value => bool.Parse(value);
    //    //        converters[typeof(DateTime)] = value => DateTime.Parse(value);

    //    //        converters[typeof(int?)] = value =>
    //    //        {
    //    //            if (value == null)
    //    //                return null;
    //    //            if (!int.TryParse(value, out int result))
    //    //                return null;
    //    //            return result;
    //    //        };
    //    //        converters[typeof(uint?)] = value =>
    //    //        {
    //    //            if (value == null)
    //    //                return null;
    //    //            if (!uint.TryParse(value, out uint v))
    //    //                return null;
    //    //            return v;
    //    //        };
    //    //        converters[typeof(long?)] = value =>
    //    //        {
    //    //            if (value == null)
    //    //                return null;
    //    //            long v;
    //    //            if (!long.TryParse(value, out v))
    //    //                return null;
    //    //            return v;
    //    //        };
    //    //        converters[typeof(ulong?)] = value =>
    //    //        {
    //    //            if (value == null)
    //    //                return null;
    //    //            if (!ulong.TryParse(value, out ulong v))
    //    //                return null;
    //    //            return v;
    //    //        };
    //    //        converters[typeof(float?)] = value =>
    //    //        {
    //    //            if (value == null)
    //    //                return null;
    //    //            if (!float.TryParse(value, out float v))
    //    //                return null;
    //    //            return v;
    //    //        };
    //    //        converters[typeof(double?)] = value =>
    //    //        {
    //    //            if (value == null)
    //    //                return null;
    //    //            if (!double.TryParse(value, out double v))
    //    //                return null;
    //    //            return v;
    //    //        };
    //    //        converters[typeof(decimal?)] = value =>
    //    //        {
    //    //            if (value == null)
    //    //                return null;
    //    //            decimal v;
    //    //            if (!decimal.TryParse(value, out v))
    //    //                return null;
    //    //            return v;
    //    //        };
    //    //        converters[typeof(bool?)] = value =>
    //    //        {
    //    //            if (value == null)
    //    //                return null;
    //    //            if (!bool.TryParse(value, out bool v))
    //    //                return null;
    //    //            return v;
    //    //        };
    //    //        converters[typeof(DateTime?)] = value =>
    //    //        {
    //    //            if (value == null)
    //    //                return null;
    //    //            if (!DateTime.TryParse(value, out DateTime v))
    //    //                return null;
    //    //            return v;
    //    //        };

    //    //        converters[typeof(object)] = value => value;
    //    //    }
    //    //    ;
    //    //    return converters;
    //    //}
    //}
}
