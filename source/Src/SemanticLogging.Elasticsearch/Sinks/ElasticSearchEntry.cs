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
    }
}
