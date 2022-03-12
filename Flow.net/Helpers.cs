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
        public static string Serialize<T>(this T source, bool beautify = false)
            => JsonConvert.SerializeObject(source, beautify ? Formatting.Indented : Formatting.None);

        public static T Deserialize<T>(this string source)
            => JsonConvert.DeserializeObject<T>(source);
    }
}
