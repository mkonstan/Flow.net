using System;
using System.Text.Json.Serialization;

namespace Flow
{
    public interface IValue
    {
        Type Type { get; }
    }
}
