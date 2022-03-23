using EquipmentSearchIndexer.Config;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenFTTH.EventSourcing;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace EquipmentSearchIndexer;

internal class EquipmentSearchIndexerHost : BackgroundService
{
    private readonly ILogger<EquipmentSearchIndexerHost> _logger;
    private readonly Settings _settings;
    private readonly IEventStore _eventStore;

    public EquipmentSearchIndexerHost(
        IEventStore eventStore,
        ILogger<EquipmentSearchIndexerHost> logger,
        Settings settings)
    {
        _eventStore = eventStore;
        _logger = logger;
        _settings = settings;
    }

    protected async override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation($"Starting {nameof(EquipmentSearchIndexerHost)}");

        try
        {
            _logger.LogInformation("Start reading all events...");
            _eventStore.DehydrateProjections();
            _logger.LogInformation("Initial event processing finish.");
            _logger.LogInformation("Start listning for new events...");
            await ListenEvents(stoppingToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex.Message, ex);
        }
    }

    private async Task ListenEvents(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000, stoppingToken).ConfigureAwait(false);

            var eventsProcessed = _eventStore.CatchUp();

            if (eventsProcessed > 0)
                _logger.LogInformation($"Processed {eventsProcessed} new events.");
        }
    }
}
