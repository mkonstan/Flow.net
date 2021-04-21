using System;
using System.Text.Json.Serialization;

namespace Flow
{
    public interface IPayload
    {
        [JsonIgnore]
        Type Type { get; }
    }

    //public class IResult
    //{
    //    [JsonIgnore]
    //    Type Type { get; }
    //}
}
