using System.Collections.Generic;

namespace Flow
{
	public interface IState : IEnumerable<KeyValuePair<string, object>>
    {
        object this[string name] { get; set; }

        IDictionary<string, object> GetState();
    }
}
