using System.Runtime.CompilerServices;
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

[assembly: InternalsVisibleTo("Flow.Data.SqlServer")]
[assembly: InternalsVisibleTo("Flow.Data.Postgres")]
[assembly: InternalsVisibleTo("FlowTest")]

namespace Flow
{
    internal static class LinqHelpers
    {
        internal static HashSet<T> ToHashSet<T>(this IEnumerable<T> source, IEqualityComparer<T> comparer)
            => new HashSet<T>(source, comparer);
    }

    public static class JsonHelpers
    {
        public static string Serialize<T>(this T source, bool beautify = false)
            => JsonConvert.SerializeObject(source, beautify ? Formatting.Indented : Formatting.None);

        public static T Deserialize<T>(this string source)
            => JsonConvert.DeserializeObject<T>(source);
    }
}
