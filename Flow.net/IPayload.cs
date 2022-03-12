using System;
using System.Text.Json.Serialization;

namespace Flow
{
    public interface IPayload
    {
        Type Type { get; }
    }
}
