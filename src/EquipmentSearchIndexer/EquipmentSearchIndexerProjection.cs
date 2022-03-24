using EquipmentSearchIndexer.Config;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenFTTH.EventSourcing;
using OpenFTTH.UtilityGraphService.Business.TerminalEquipments.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Typesense;

namespace EquipmentSearchIndexer;

record TypesenseEquipment(Guid Id, string Name);
record BulkEquipment(Guid Id, string? Name, Guid SpecificationId);

internal class EquipmentSearchIndexerProjection : ProjectionBase
{
    private readonly ILogger<EquipmentSearchIndexerProjection> _logger;
    private readonly ITypesenseClient _typesense;
    private readonly Settings _settings;
    private bool _bulkMode = true;
    private readonly Dictionary<Guid, string> _specifications = new();
    private readonly Dictionary<Guid, BulkEquipment> _bulkEquipments = new();

    public EquipmentSearchIndexerProjection(
        ILogger<EquipmentSearchIndexerProjection> logger,
        ITypesenseClient typesense,
        IOptions<Settings> settings)
    {
        _logger = logger;
        _typesense = typesense;
        _settings = settings.Value;
        ProjectEventAsync<TerminalEquipmentPlacedInNodeContainer>(Project);
        ProjectEventAsync<TerminalEquipmentNamingInfoChanged>(Project);
        ProjectEventAsync<TerminalEquipmentSpecificationAdded>(Project);
        ProjectEventAsync<TerminalEquipmentSpecificationChanged>(Project);
    }

    public override async Task DehydrationFinishAsync()
    {
        _logger.LogInformation("Starting bulk insert.");

        // We do this because we want to avoid invalid configurations.
        // Because they can cause unexpected behaviours.
        if (_settings.SpecificationNames.Count != _specifications.Count)
        {
            throw new ArgumentException("Could not find all specifications that is registered for insert.");
        }

        var batchSize = 100;
        var batch = new List<TypesenseEquipment>();
        foreach (var bulkEquipment in _bulkEquipments)
        {
            // We only want to process equipments that has the configured specification-names.
            if (!_specifications.ContainsKey(bulkEquipment.Value.SpecificationId))
                continue;

            // We don't want to insert empty named equipment into Typesense.
            if (!string.IsNullOrEmpty(bulkEquipment.Value.Name))
            {
                var equipment = new TypesenseEquipment(bulkEquipment.Value.Id, bulkEquipment.Value.Name);
                batch.Add(equipment);

                if (batch.Count == batchSize)
                {
                    _logger.LogInformation($"Bulk inserting {batch.Count}");
                    await _typesense.ImportDocuments(_settings.UniqueCollectionName, batch, batchSize).ConfigureAwait(false);
                    batch.Clear();
                }
            }
            else
            {
                _logger.LogDebug($"Could not process equipment with id: {bulkEquipment.Key}");
            }
        }

        // Import rest
        if (batch.Count > 0)
        {
            _logger.LogInformation($"Bulk inserting {batch.Count}");
            await _typesense.ImportDocuments(_settings.UniqueCollectionName, batch).ConfigureAwait(false);
        }

        _bulkEquipments.Clear();
        _bulkMode = false;
    }

    private async Task Project(IEventEnvelope eventEnvelope)
    {
        if (_bulkMode)
        {
            await ProjectBulk(eventEnvelope).ConfigureAwait(false);
        }
        else
        {
            await ProjectCatchUp(eventEnvelope).ConfigureAwait(false);
        }
    }

    private async Task ProjectBulk(IEventEnvelope eventEnvelope)
    {
        switch (eventEnvelope.Data)
        {
            case (TerminalEquipmentPlacedInNodeContainer @event):
                await HandleBulk(@event).ConfigureAwait(false);
                break;
            case (TerminalEquipmentNamingInfoChanged @event):
                await HandleBulk(@event).ConfigureAwait(false);
                break;
            case (TerminalEquipmentSpecificationAdded @event):
                // We use Handle here since Bulk and CatchUp is the same.
                await Handle(@event).ConfigureAwait(false);
                break;
            case (TerminalEquipmentSpecificationChanged @event):
                await HandleBulk(@event).ConfigureAwait(false);
                break;
            default:
                throw new ArgumentException($"Could not handle typeof '{eventEnvelope.Data.GetType().Name}'");
        }
    }

    private async Task ProjectCatchUp(IEventEnvelope eventEnvelope)
    {
        switch (eventEnvelope.Data)
        {
            case (TerminalEquipmentPlacedInNodeContainer @event):
                await HandleCatchUp(@event).ConfigureAwait(false);
                break;
            case (TerminalEquipmentNamingInfoChanged @event):
                await HandleCatchUp(@event).ConfigureAwait(false);
                break;
            case (TerminalEquipmentSpecificationAdded @event):
                // We use Handle here since Bulk and CatchUp is the same.
                await Handle(@event).ConfigureAwait(false);
                break;
            case (TerminalEquipmentSpecificationChanged @event):
                await HandleCatchUp(@event).ConfigureAwait(false);
                break;
            default:
                throw new ArgumentException($"Could not handle typeof '{eventEnvelope.Data.GetType().Name}'");
        }
    }

    private async Task HandleBulk(TerminalEquipmentPlacedInNodeContainer @event)
    {
        if (!string.IsNullOrWhiteSpace(@event.Equipment.Name))
        {
            var equipment = new BulkEquipment(
                @event.Equipment.Id,
                @event.Equipment.Name,
                @event.Equipment.SpecificationId);
            _bulkEquipments.Add(@event.Equipment.Id, equipment);
        }
        await Task.CompletedTask;
    }

    private async Task HandleBulk(TerminalEquipmentNamingInfoChanged @event)
    {
        var equipment = _bulkEquipments[@event.TerminalEquipmentId];
        equipment = equipment with { Name = @event.NamingInfo?.Name };
        _bulkEquipments[equipment.Id] = equipment;
        await Task.CompletedTask;
    }

    private async Task HandleBulk(TerminalEquipmentSpecificationChanged @event)
    {
        var equipment = _bulkEquipments[@event.TerminalEquipmentId];
        equipment = equipment with { SpecificationId = @event.NewSpecificationId };
        _bulkEquipments[equipment.Id] = equipment;
        await Task.CompletedTask;
    }

    private async Task HandleCatchUp(TerminalEquipmentPlacedInNodeContainer @event)
    {
        if (!string.IsNullOrWhiteSpace(@event.Equipment.Name))
        {
            var equipment = new TypesenseEquipment(@event.Equipment.Id, @event.Equipment.Name);
            await _typesense.UpsertDocument(_settings.UniqueCollectionName, equipment)
                .ConfigureAwait(false);
        }
    }

    private async Task HandleCatchUp(TerminalEquipmentNamingInfoChanged @event)
    {
        if (!string.IsNullOrWhiteSpace(@event.NamingInfo?.Name))
        {
            var equipment = new TypesenseEquipment(@event.TerminalEquipmentId, @event.NamingInfo.Name);
            await _typesense.UpdateDocument(_settings.UniqueCollectionName, equipment.Id.ToString(), equipment)
                .ConfigureAwait(false);
        }
    }

    private async Task HandleCatchUp(TerminalEquipmentSpecificationChanged @event)
    {
        _logger.LogInformation($"Got {nameof(TerminalEquipmentSpecificationChanged)}");
        await Task.CompletedTask;
    }

    private async Task Handle(TerminalEquipmentSpecificationAdded @event)
    {
        if (_settings.SpecificationNames.Contains(@event.Specification.Name))
        {
            _logger.LogInformation($"Adds {nameof(TerminalEquipmentSpecificationAdded)} {@event.Specification.Name}");
            _specifications.Add(@event.Specification.Id, @event.Specification.Name);
        }
        await Task.CompletedTask;
    }
}
