using Microsoft.Extensions.Logging;
using OpenFTTH.EventSourcing;
using OpenFTTH.UtilityGraphService.Business.TerminalEquipments.Events;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Typesense;

namespace EquipmentSearchIndexer;

internal class EquipmentSearchIndexerProjection : ProjectionBase
{
    private readonly ILogger<EquipmentSearchIndexerProjection> _logger;
    private readonly Dictionary<Guid, string> _specifications = new();
    private readonly ITypesenseClient _typesense;

    public EquipmentSearchIndexerProjection(
        ILogger<EquipmentSearchIndexerProjection> logger,
        ITypesenseClient typesense)
    {
        _logger = logger;
        _typesense = typesense;
        ProjectEventAsync<TerminalEquipmentPlacedInNodeContainer>(Project);
        ProjectEventAsync<TerminalEquipmentNamingInfoChanged>(Project);
        ProjectEventAsync<TerminalEquipmentSpecificationAdded>(Project);
        ProjectEventAsync<TerminalEquipmentSpecificationChanged>(Project);
    }

    private async Task Project(IEventEnvelope eventEnvelope)
    {
        switch (eventEnvelope.Data)
        {
            case (TerminalEquipmentPlacedInNodeContainer @event):
                await Handle(@event).ConfigureAwait(false);
                break;
            case (TerminalEquipmentNamingInfoChanged @event):
                await Handle(@event).ConfigureAwait(false);
                break;
            case (TerminalEquipmentSpecificationAdded @event):
                await Handle(@event).ConfigureAwait(false);
                break;
            case (TerminalEquipmentSpecificationChanged @event):
                await Handle(@event).ConfigureAwait(false);
                break;
            default:
                throw new ArgumentException($"Could not handle typeof '{eventEnvelope.Data.GetType().Name}'");
        }
    }

    private async Task Handle(TerminalEquipmentPlacedInNodeContainer @event)
    {
        _logger.LogInformation($"Got {nameof(TerminalEquipmentPlacedInNodeContainer)}");
        await Task.CompletedTask;
    }

    private async Task Handle(TerminalEquipmentNamingInfoChanged @event)
    {
        _logger.LogInformation($"Got {nameof(TerminalEquipmentNamingInfoChanged)}");
        await Task.CompletedTask;
    }

    private async Task Handle(TerminalEquipmentSpecificationAdded @event)
    {
        _logger.LogInformation($"Got {nameof(TerminalEquipmentSpecificationAdded)}");
        _specifications.TryAdd(@event.Specification.Id, @event.Specification.Name);
        await Task.CompletedTask;
    }

    private async Task Handle(TerminalEquipmentSpecificationChanged @event)
    {
        _logger.LogInformation($"Got {nameof(TerminalEquipmentSpecificationChanged)}");
        await Task.CompletedTask;
    }
}
