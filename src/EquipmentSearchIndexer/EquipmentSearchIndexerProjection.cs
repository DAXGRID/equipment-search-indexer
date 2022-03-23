using EquipmentSearchIndexer.Config;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenFTTH.EventSourcing;
using OpenFTTH.UtilityGraphService.Business.TerminalEquipments.Events;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EquipmentSearchIndexer;

internal class EquipmentSearchIndexerProjection : ProjectionBase
{
    private readonly ILogger<EquipmentSearchIndexerProjection> _logger;
    private readonly Settings _settings;
    private readonly Dictionary<Guid, string> _specifications = new();

    public EquipmentSearchIndexerProjection(
        ILogger<EquipmentSearchIndexerProjection> logger,
        IOptions<Settings> settings)
    {
        _logger = logger;
        _settings = settings.Value;
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
                Handle(@event);
                break;
            case (TerminalEquipmentNamingInfoChanged @event):
                Handle(@event);
                break;
            case (TerminalEquipmentSpecificationAdded @event):
                Handle(@event);
                break;
            case (TerminalEquipmentSpecificationChanged @event):
                Handle(@event);
                break;
            default:
                throw new ArgumentException($"Could not handle typeof '{eventEnvelope.Data.GetType().Name}'");
        }
    }

    private async Task Handle(TerminalEquipmentPlacedInNodeContainer @event)
    {
    }

    private async Task Handle(TerminalEquipmentNamingInfoChanged @event)
    {
    }

    private async Task Handle(TerminalEquipmentSpecificationAdded @event)
    {
        _specifications.TryAdd(@event.Specification.Id, @event.Specification.Name);
    }

    private async Task Handle(TerminalEquipmentSpecificationChanged @event)
    {
    }
}
