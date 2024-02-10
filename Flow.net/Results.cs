using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Flow
{
    public class ValueResult<T> : IValue
    {
        public ValueResult(T value) { Value = value; }

        public T Value { get; }

        public virtual Type Type => typeof(ValueResult<T>);
    }

    public class ErrorResult : IValue
    {
        protected ErrorResult(Exception error) { Error = error; }

        Exception Error { get; }

        public Type Type => typeof(ErrorResult);
    }

    public sealed class NullResult : IValue
    {
        public static NullResult Instance = new NullResult();

        public Type Type => typeof(NullResult);
    }

    public class FilePath : IValue
    {
        public FilePath(string filePath) { Path = filePath; }

        public string Path { get; }

        public Type Type => typeof(FilePath);
    }

    public class ObjectResult : ValueResult<object>
    {
        public ObjectResult(object value) : base(value)
        {
        }

        public override Type Type => typeof(ObjectResult);
    }

    public abstract class ValueCollection<TValue> : IValue, IEnumerable<TValue>
    {
        private readonly IEnumerable<TValue> _values;

        public ValueCollection(IEnumerable<TValue> collection)
        {
            _values = collection.ToArray();
        }

        public IEnumerator<TValue> GetEnumerator() => _values.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public abstract Type Type { get; }
    }

    public class DictionaryCollection : ValueCollection<IDictionary<string, object>>
    {
        public DictionaryCollection(IEnumerable<IDictionary<string, object>> collection)
            : base(collection)
        { }

        public override Type Type => typeof(DictionaryCollection);
    }

    public class ExpandoCollection : ValueCollection<dynamic>
    {
        public ExpandoCollection(IEnumerable<dynamic> collection)
            :base(collection)
        { }

        public override Type Type => typeof(ExpandoCollection);
    }

    public class PayloadCollection : ValueCollection<IValue>
    {
        public PayloadCollection(IEnumerable<dynamic> collection)
            : this(collection.Select(c => new ObjectResult(c)))
        { }

        public PayloadCollection(IEnumerable<IValue> collection)
            : base(collection)
        { }

        public override Type Type => typeof(PayloadCollection);
    }

    public class FilePathCollection : PayloadCollection
    {
        public FilePathCollection(IEnumerable<string> paths) : base(paths.Select(p => new FilePath(p)))
        {
        }
    }
}
