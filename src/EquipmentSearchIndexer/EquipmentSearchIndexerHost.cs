using EquipmentSearchIndexer.Config;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenFTTH.EventSourcing;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Typesense;

namespace EquipmentSearchIndexer;

internal class EquipmentSearchIndexerHost : BackgroundService
{
    private readonly ILogger<EquipmentSearchIndexerHost> _logger;
    private readonly IEventStore _eventStore;
    private readonly ITypesenseClient _typesenseClient;
    private readonly Settings _settings;

    public EquipmentSearchIndexerHost(
        IEventStore eventStore,
        ILogger<EquipmentSearchIndexerHost> logger,
        ITypesenseClient typesenseClient,
        IOptions<Settings> settings)
    {
        _eventStore = eventStore;
        _logger = logger;
        _typesenseClient = typesenseClient;
        _settings = settings.Value;
    }

    protected async override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation($"Starting {nameof(EquipmentSearchIndexerHost)}");
        var processableNames = String.Join(", ", _settings.SpecificationNames);
        _logger.LogInformation($"Processing with specification names: '{processableNames}'");
        _logger.LogInformation($"Processing with collection alias: '{_settings.CollectionAliasName}'");
        _logger.LogInformation($"Processing with unique collection name: '{_settings.UniqueCollectionName}'");

        var collectionName = _settings.UniqueCollectionName;
        var alias = _settings.CollectionAliasName;
        try
        {
            _logger.LogInformation($"Creating Typesense collection '{collectionName}'.");
            var schema = CreateSchema(collectionName);
            await _typesenseClient.CreateCollection(schema).ConfigureAwait(false);

            _logger.LogInformation("Start reading all events.");
            await _eventStore.DehydrateProjectionsAsync().ConfigureAwait(false);
            _logger.LogInformation("Initial event processing finished.");

            _logger.LogInformation($"Switching alias '{alias}' to '{collectionName}'");
            await _typesenseClient.UpsertCollectionAlias(
                alias, new CollectionAlias(collectionName)).ConfigureAwait(false);

            _logger.LogInformation($"Deleteing old collections.");
            await DeleteOldCollections().ConfigureAwait(false);

            _logger.LogInformation($"Marking service as healthy.");
            File.Create("/tmp/healthy");

            _logger.LogInformation("Start listening for new events.");
            await ListenEvents(stoppingToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex.Message, ex);
            throw;
        }
        finally
        {
            _logger.LogInformation($"Shutting down.");
        }
    }

    private async Task DeleteOldCollections()
    {
        var collections = await _typesenseClient.RetrieveCollections().ConfigureAwait(false);
        var oldCollections = GetOldCollections(_settings.UniqueCollectionName, _settings.CollectionAliasName, collections);
        foreach (var oldCollection in oldCollections)
        {
            _logger.LogInformation($"Deleteting old collection '{oldCollection}'");
            await _typesenseClient.DeleteCollection(oldCollection).ConfigureAwait(false);
        }
    }

    private async Task ListenEvents(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000, stoppingToken).ConfigureAwait(false);
            var eventsProcessed = await _eventStore.CatchUpAsync().ConfigureAwait(false);
            if (eventsProcessed > 0)
                _logger.LogInformation($"Processed {eventsProcessed} new events.");
        }
    }

    private static Schema CreateSchema(string collectionName)
        => new Schema(
            collectionName,
            new List<Field>
            {
                new Field("id", FieldType.String, false, false, true),
                new Field("name", FieldType.String, false, false, true),
            });

    private static IEnumerable<string> GetOldCollections(
        string newCollectionName, string collectionPrefix, List<CollectionResponse> collections)
        => collections
        .Where(x => x.Name.StartsWith(collectionPrefix) && x.Name != newCollectionName)
        .Select(x => x.Name);
}
