using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Sinks
{
    /// <summary>
    /// Wrapper for elastic search entries
    /// </summary>
    public class ElasticSearchEntry : EventEntry
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ElasticSearchEntry" /> class.
        /// </summary>
        /// <param name="entry"></param>
        public ElasticSearchEntry(EventEntry entry) : base(entry.ProviderId, entry.EventId, entry.FormattedMessage, entry.Payload, entry.Timestamp, entry.ProcessId, entry.ThreadId, entry.ActivityId, entry.RelatedActivityId, entry.Schema)
        {
        }

        /// <summary>
        /// Gets or sets name of the source of the event entry
        /// </summary>
        public string InstanceName { get; set; }

        /// <summary>
        /// Gets a string representation of event entry level
        /// </summary>
        public string LevelString
        {
            get { return Schema.Level.ToString(); }
        }

        /// <summary>
        /// Gets prettified payload in the form of key-value pairs
        /// </summary>
        public ExpandoObject PrettyPayload
        {
            get
            {
                var result = new ExpandoObject();
                IDictionary<string, object> dict = result;
                foreach (var pair in Schema.Payload.Zip(Payload, (key, value) => new { key, value }))
                {
                    //Convert unsigned value to string to allow elasticsearch to handle it
                    if (pair.value is ulong || pair.value is uint)
                    {
                        dict[pair.key] = pair.value.ToString();
                    }
                    else
                    {
                        dict[pair.key] = pair.value;
                    }
                }
                return result;
            }
        }
    }
}
