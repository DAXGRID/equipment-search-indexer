using EquipmentSearchIndexer.Config;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenFTTH.EventSourcing;
using OpenFTTH.UtilityGraphService.Business.TerminalEquipments.Events;
using System;

namespace EquipmentSearchIndexer;

internal class EquipmentSearchIndexerProjection : ProjectionBase
{
    private readonly ILogger<EquipmentSearchIndexerProjection> _logger;
    private readonly Settings _settings;

    public EquipmentSearchIndexerProjection(
        ILogger<EquipmentSearchIndexerProjection> logger,
        IOptions<Settings> settings)
    {
        _logger = logger;
        _settings = settings.Value;
        ProjectEvent<TerminalEquipmentPlacedInNodeContainer>(Project);
        ProjectEvent<TerminalEquipmentNamingInfoChanged>(Project);
        ProjectEvent<TerminalEquipmentSpecificationAdded>(Project);
        ProjectEvent<TerminalEquipmentSpecificationChanged>(Project);
    }

    private void Project(IEventEnvelope eventEnvelope)
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

    private void Handle(TerminalEquipmentPlacedInNodeContainer @event)
    {
        // Handle
    }

    private void Handle(TerminalEquipmentNamingInfoChanged @event)
    {

    }

    private void Handle(TerminalEquipmentSpecificationAdded @event)
    {

    }

    private void Handle(TerminalEquipmentSpecificationChanged @event)
    {

    }
}
