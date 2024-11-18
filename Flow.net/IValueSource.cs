using System;
using System.Text.Json.Serialization;

namespace Flow
{
    public interface IValueSource
    {
        Type Type { get; }
    }
}
