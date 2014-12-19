// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Properties;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Schema;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Utility;
using Nest;
using Newtonsoft.Json.Linq;

namespace Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Sinks
{
    /// <summary>
    /// Sink that asynchronously writes entries to a Elasticsearch server.
    /// </summary>
    public class ElasticsearchSink : IObserver<EventEntry>, IDisposable
    {
        private const string BulkServiceOperationPath = "/_bulk";

        private readonly BufferedEventPublisher<EventEntry> bufferedPublisher;
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        private readonly string index;
        private readonly string type;
        private readonly string instanceName;

        private readonly bool flattenPayload;

        private readonly Uri elasticsearchUrl;
        private readonly TimeSpan onCompletedTimeout;

        /// <summary>
        /// Initializes a new instance of the <see cref="ElasticsearchSink"/> class with the specified connection string and table address.
        /// </summary>
        /// <param name="instanceName">The name of the instance originating the entries.</param>
        /// <param name="connectionString">The connection string for the storage account.</param>
        /// <param name="index">Index name prefix formatted as index-{0:yyyy.MM.DD}.</param>
        /// <param name="type">Elasticsearch entry type.</param>
        /// <param name="flattenPayload">Flatten the payload collection when serializing event entries</param>
        /// <param name="bufferInterval">The buffering interval to wait for events to accumulate before sending them to Elasticsearch.</param>
        /// <param name="bufferingCount">The buffering event entry count to wait before sending events to Elasticsearch </param>
        /// <param name="maxBufferSize">The maximum number of entries that can be buffered while it's sending to Windows Azure Storage before the sink starts dropping entries.</param>
        /// <param name="onCompletedTimeout">Defines a timeout interval for when flushing the entries after an <see cref="OnCompleted"/> call is received and before disposing the sink.
        /// This means that if the timeout period elapses, some event entries will be dropped and not sent to the store. Normally, calling <see cref="IDisposable.Dispose"/> on 
        /// the <see cref="System.Diagnostics.Tracing.EventListener"/> will block until all the entries are flushed or the interval elapses.
        /// If <see langword="null"/> is specified, then the call will block indefinitely until the flush operation finishes.</param>
        public ElasticsearchSink(string instanceName, string connectionString, string index, string type, bool? flattenPayload, TimeSpan bufferInterval,
            int bufferingCount, int maxBufferSize, TimeSpan onCompletedTimeout)
        {
            Guard.ArgumentNotNullOrEmpty(instanceName, "instanceName");
            Guard.ArgumentNotNullOrEmpty(connectionString, "connectionString");
            Guard.ArgumentNotNullOrEmpty(index, "index");
            Guard.ArgumentNotNullOrEmpty(type, "type");
            Guard.ArgumentIsValidTimeout(onCompletedTimeout, "onCompletedTimeout");
            Guard.ArgumentGreaterOrEqualThan(0, bufferingCount, "bufferingCount");

            if (Regex.IsMatch(index, "[\\\\/*?\",<>|\\sA-Z]"))
            {
                throw new ArgumentException(Resource.InvalidElasticsearchIndexNameError, "index");
            }

            this.onCompletedTimeout = onCompletedTimeout;

            this.instanceName = instanceName;
            this.flattenPayload = flattenPayload ?? true;
            this.elasticsearchUrl = new Uri(connectionString);
            this.index = index;
            this.type = type;
            var sinkId = string.Format(CultureInfo.InvariantCulture, "ElasticsearchSink ({0})", instanceName);
            bufferedPublisher = BufferedEventPublisher<EventEntry>.CreateAndStart(sinkId, PublishEventsAsync, bufferInterval,
                bufferingCount, maxBufferSize, cancellationTokenSource.Token);
        }

        /// <summary>
        /// Releases all resources used by the current instance of the <see cref="ElasticsearchSink"/> class.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Notifies the observer that the provider has finished sending push-based notifications.
        /// </summary>
        public void OnCompleted()
        {
            FlushSafe();
            Dispose();
        }

        /// <summary>
        /// Provides the sink with new data to write.
        /// </summary>
        /// <param name="value">The current entry to write to Windows Azure.</param>
        public void OnNext(EventEntry value)
        {
            if (value == null)
            {
                return;
            }

            bufferedPublisher.TryPost(value);
        }

        /// <summary>
        /// Notifies the observer that the provider has experienced an error condition.
        /// </summary>
        /// <param name="error">An object that provides additional information about the error.</param>
        public void OnError(Exception error)
        {
            FlushSafe();
            Dispose();
        }

        /// <summary>
        /// Finalizes an instance of the <see cref="ElasticsearchSink"/> class.
        /// </summary>
        ~ElasticsearchSink()
        {
            Dispose(false);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <param name="disposing">A value indicating whether or not the class is disposing.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2213:DisposableFieldsShouldBeDisposed",
            MessageId = "cancellationTokenSource", Justification = "Token is canceled")]
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                cancellationTokenSource.Cancel();
                bufferedPublisher.Dispose();
            }
        }

        /// <summary>
        /// Causes the buffer to be written immediately.
        /// </summary>
        /// <returns>The Task that flushes the buffer.</returns>
        public Task FlushAsync()
        {
            return bufferedPublisher.FlushAsync();
        }

        internal async Task<int> PublishEventsAsync(IList<EventEntry> collection)
        {
            try
            {
                var settings = new ConnectionSettings(this.elasticsearchUrl);
                var client = new ElasticClient(settings);

                await VerifyIndexExists(client);

                var descriptor = new BulkDescriptor();
                foreach (var entry in collection)
                {
                    var localEntry = new ElasticSearchEntry(entry)
                    {
                        InstanceName = instanceName,
                    };
                    descriptor.Index(CreateIndexDescriptor(localEntry));
                }

                //send bulk request to elastic search: may require cancellation
                var response = await client.BulkAsync(descriptor);

                // If there is an exception
                if (response.ServerError != null)
                {
                    if (response.ServerError.Status == (int)HttpStatusCode.BadRequest)
                    {
                        var messagesDiscarded = collection.Count();

                        var serverErrorMessage = string.Format(
                            "Server error type: {0}, message: {1}", response.ServerError.ExceptionType, response.ServerError.Error);

                        // We are unable to write the batch of event entries - Possible poison message
                        // I don't like discarding events but we cannot let a single malformed event prevent others from being written
                        // We might want to consider falling back to writing entries individually here
                        SemanticLoggingEventSource.Log.ElasticsearchSinkWriteEventsFailedAndDiscardsEntries(messagesDiscarded, serverErrorMessage);

                        return messagesDiscarded;
                    }

                    // This will leave the messages in the buffer
                    return 0;
                }

                var itemErrors = 0;
                foreach (var item in response.ItemsWithErrors)
                {
                    ++itemErrors;
                    SemanticLoggingEventSource.Log.EventEntrySerializePayloadFailed(item.Error);
                }

                // If the response return items collection
                if (response.Items != null)
                {
                    // NOTE: This only works with Elasticsearch 1.0
                    // Alternatively we could query ES as part of initialization check results or fall back to trying <1.0 parsing
                    // We should also consider logging errors for individual entries
                    return response.Items.Count(item => item.IsValid && item.Status.Equals(201)) + itemErrors;

                    // Pre-1.0 Elasticsearch
                    // return items.Count(t => t["create"]["ok"].Value<bool>().Equals(true));
                }

                return 0;
            }
            catch (OperationCanceledException)
            {
                return 0;
            }
            catch (Exception ex)
            {
                // Although this is generally considered an anti-pattern this is not logged upstream and we have context
                SemanticLoggingEventSource.Log.ElasticsearchSinkWriteEventsFailed(ex.ToString());
                throw;
            }
        }

        private async Task VerifyIndexExists(IElasticClient client)
        {
            var indexExistsResponse = await client.IndexExistsAsync(index);
            VerifyResponse(indexExistsResponse);

            if (!indexExistsResponse.Exists)
            {
                var createIndexResponse = await client.CreateIndexAsync(index);
                if (!createIndexResponse.IsValid)
                {
                    throw new ElasticsearchServerException(createIndexResponse.ServerError);
                }
            }

            var typeExistsResponse = await client.TypeExistsAsync(index, type);
            VerifyResponse(typeExistsResponse);

            if (!typeExistsResponse.Exists)
            {
                var createMappingResponse = await client.MapAsync<ElasticSearchEntry>(CreateMapping);
                if (!createMappingResponse.IsValid)
                {
                    throw new ElasticsearchServerException(createMappingResponse.ServerError);
                }
            }
        }

        private static void VerifyResponse(IExistsResponse existsResponse)
        {
            if (!existsResponse.IsValid)
            {
                throw new ElasticsearchServerException(existsResponse.ServerError);
            }
        }

        private PutMappingDescriptor<ElasticSearchEntry> CreateMapping(PutMappingDescriptor<ElasticSearchEntry> putMappingDescriptor)
        {
            return putMappingDescriptor
                .Index(index)
                .Type(type)
                .MapFromAttributes()
                .Properties(properties => properties
                    .Object<EventSchema>(o => o
                        .Name(entry => entry.Schema)
                        .MapFromAttributes()
                        .Properties(props => props
                            .MultiField(field => field
                                .Name(schema => schema.EventName)
                                .Fields(fields => fields
                                    .String(s => s.Name(entry => entry.EventName).Index(FieldIndexOption.Analyzed))
                                    .String(s => s.Name("eventName_").Index(FieldIndexOption.NotAnalyzed))
                                )
                            )
                            .MultiField(field => field
                                .Name(schema => schema.KeywordsDescription)
                                .Fields(fields => fields
                                    .String(s => s.Name(schema => schema.KeywordsDescription).Index(FieldIndexOption.Analyzed))
                                    .String(s => s.Name("keywordsDescription_").Index(FieldIndexOption.NotAnalyzed))
                                )
                            )
                            .MultiField(field => field
                                .Name(schema => schema.ProviderName)
                                .Fields(fields => fields
                                    .String(s => s.Name(schema => schema.ProviderName).Index(FieldIndexOption.Analyzed))
                                    .String(s => s.Name("providerName_").Index(FieldIndexOption.NotAnalyzed))
                                )
                            )
                            .Object<object>(field => field.Name(schema => schema.Payload).Enabled(false))
                        )
                    )
                    .Object<object>(o => o
                        .Name(entry => entry.PrettyPayload)
                        .Properties(props => props
                            .MultiField(field => field
                                .Name("batch_text")
                                .Fields(fields => fields
                                    .String(s => s.Name("batch_text").Index(FieldIndexOption.Analyzed))
                                    .String(s => s.Name("batch_text_").Index(FieldIndexOption.NotAnalyzed))
                                )
                            )
                            .MultiField(field => field
                                .Name("showplan_xml")
                                .Fields(fields => fields
                                    .String(s => s.Name("showplan_xml").Index(FieldIndexOption.Analyzed))
                                    .String(s => s.Name("showplan_xml_").Index(FieldIndexOption.NotAnalyzed))
                                )
                            )
                            .String(field => field.Name("connectionObj").Index(FieldIndexOption.NotAnalyzed))
                        )
                    )
                    .String(s => s.Name(entry => entry.InstanceName).Index(FieldIndexOption.NotAnalyzed))
                    .String(s => s.Name(entry => entry.LevelString).Index(FieldIndexOption.NotAnalyzed))
                    .Object<object>(field => field.Name(entry => entry.Payload).Enabled(false))
                );
        }

        private Func<BulkIndexDescriptor<ElasticSearchEntry>, BulkIndexDescriptor<ElasticSearchEntry>> CreateIndexDescriptor(ElasticSearchEntry entry)
        {
            return selector => selector.Document(entry).Index(index).Type(type);
        }

        private void FlushSafe()
        {
            try
            {
                FlushAsync().Wait(onCompletedTimeout);
            }
            catch (AggregateException ex)
            {
                // Flush operation will already log errors. Never expose this exception to the observable.
                ex.Handle(e => e is FlushFailedException);
            }
        }
    }
}