using EquipmentSearchIndexer.Config;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenFTTH.EventSourcing;
using System;
using System.Collections.Generic;
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
        _logger.LogInformation($"Processing following specification names: '{processableNames}'");

        var collectionName = _settings.UniqueCollectionName;
        var aliasName = "equipments";
        try
        {
            _logger.LogInformation($"Creating Typesense collection '{collectionName}'.");
            await CreateEquipmentCollection(collectionName).ConfigureAwait(false);

            _logger.LogInformation("Start reading all events.");
            await _eventStore.DehydrateProjectionsAsync().ConfigureAwait(false);
            _logger.LogInformation("Initial event processing finished.");

            _logger.LogInformation($"Switching alias '{aliasName}' to '{collectionName}'");
            await _typesenseClient.UpsertCollectionAlias(aliasName, new CollectionAlias(collectionName))
                .ConfigureAwait(false);

            _logger.LogInformation($"Marking service as healthy.");
            MarkAsHealthy();

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
            _logger.LogInformation($"Deleting collection '{collectionName}'.");
            await _typesenseClient.DeleteCollection(collectionName).ConfigureAwait(false);
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

    private async Task CreateEquipmentCollection(string collectionName)
    {
        var schema = new Schema
        {
            Name = collectionName,
            Fields = new List<Field>
            {
                new Field("id", FieldType.String, false, false, true),
                new Field("name", FieldType.String, false, false, true),
            },
        };

        await _typesenseClient.CreateCollection(schema).ConfigureAwait(false);
    }

    private void MarkAsHealthy()
    {
        File.Create("/tmp/healthy");
    }
}
