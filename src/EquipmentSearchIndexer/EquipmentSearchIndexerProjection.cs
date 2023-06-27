using EquipmentSearchIndexer.Config;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenFTTH.EventSourcing;
using OpenFTTH.UtilityGraphService.Business.TerminalEquipments.Events;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Typesense;

namespace EquipmentSearchIndexer;

record TypesenseEquipment(Guid Id, string Name);
record Equipment(Guid Id, string? Name, Guid SpecificationId);

internal class EquipmentSearchIndexerProjection : ProjectionBase
{
    private readonly ILogger<EquipmentSearchIndexerProjection> _logger;
    private readonly ITypesenseClient _typesense;
    private readonly Settings _settings;
    private bool _bulkMode = true;
    private readonly Dictionary<Guid, string> _specifications = new();
    private readonly Dictionary<Guid, Equipment> _equipments = new();

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
        ProjectEventAsync<TerminalEquipmentRemoved>(Project);
    }

    public override async Task DehydrationFinishAsync()
    {
        _logger.LogInformation("Starting bulk insert.");

        // We do this because we want to avoid invalid configurations.
        // Because they can cause unexpected behaviours.
        if (_settings.SpecificationNames.Count != _specifications.Count)
            throw new ArgumentException("Could not find all specifications that is registered for insert.");

        var insertedCounter = 0;
        var batchSize = 100;
        var batch = new List<TypesenseEquipment>();
        foreach (var equipment in _equipments)
        {
            // We only want to process equipments that has the configured specification-names
            // And name is not string or empty.
            if (_specifications.ContainsKey(equipment.Value.SpecificationId) &&
                !string.IsNullOrEmpty(equipment.Value.Name))
            {
                var document = new TypesenseEquipment(equipment.Value.Id, equipment.Value.Name);
                batch.Add(document);

                if (batch.Count == batchSize)
                {
                    insertedCounter += batchSize;
                    _logger.LogDebug($"Bulk inserting {batch.Count}");
                    await _typesense.ImportDocuments(_settings.UniqueCollectionName, batch, batchSize).ConfigureAwait(false);
                    batch.Clear();
                }
            }
        }

        // Import remaining batch.
        if (batch.Count > 0)
        {
            insertedCounter += batch.Count;
            _logger.LogInformation($"Bulk inserting {batch.Count}");
            await _typesense.ImportDocuments(_settings.UniqueCollectionName, batch).ConfigureAwait(false);
        }

        _logger.LogInformation($"Inserted a total of {insertedCounter} doing bulk.");

        _bulkMode = false;
    }

    private async Task Project(IEventEnvelope eventEnvelope)
    {
        if (_bulkMode)
        {
            ProjectBulk(eventEnvelope);
        }
        else
        {
            await ProjectCatchUp(eventEnvelope).ConfigureAwait(false);
        }
    }

    private void ProjectBulk(IEventEnvelope eventEnvelope)
    {
        switch (eventEnvelope.Data)
        {
            case (TerminalEquipmentPlacedInNodeContainer @event):
                HandleBulk(@event);
                break;
            case (TerminalEquipmentNamingInfoChanged @event):
                HandleBulk(@event);
                break;
            case (TerminalEquipmentSpecificationAdded @event):
                HandleBulk(@event);
                break;
            case (TerminalEquipmentSpecificationChanged @event):
                HandleBulk(@event);
                break;
            case (TerminalEquipmentRemoved @event):
                HandleBulk(@event);
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
            case (TerminalEquipmentSpecificationChanged @event):
                await HandleCatchUp(@event).ConfigureAwait(false);
                break;
            case (TerminalEquipmentRemoved @event):
                await HandleCatchUp(@event).ConfigureAwait(false);
                break;
            default:
                throw new ArgumentException($"Could not handle typeof '{eventEnvelope.Data.GetType().Name}'");
        }
    }

    private void HandleBulk(TerminalEquipmentPlacedInNodeContainer @event)
    {
        var equipment = new Equipment(
            @event.Equipment.Id,
            @event.Equipment.Name,
            @event.Equipment.SpecificationId);

        _equipments.Add(@event.Equipment.Id, equipment);
    }

    private void HandleBulk(TerminalEquipmentNamingInfoChanged @event)
    {
        var equipment = _equipments[@event.TerminalEquipmentId];
        equipment = equipment with { Name = @event.NamingInfo?.Name };
        _equipments[equipment.Id] = equipment;
    }

    private void HandleBulk(TerminalEquipmentSpecificationChanged @event)
    {
        var equipment = _equipments[@event.TerminalEquipmentId];
        equipment = equipment with { SpecificationId = @event.NewSpecificationId };
        _equipments[equipment.Id] = equipment;
    }

    private void HandleBulk(TerminalEquipmentSpecificationAdded @event)
    {
        if (_settings.SpecificationNames.Contains(@event.Specification.Name))
        {
            _specifications.Add(@event.Specification.Id, @event.Specification.Name);
        }
    }

    private void HandleBulk(TerminalEquipmentRemoved @event)
    {
        _equipments.Remove(@event.TerminalEquipmentId);
    }

    private async Task HandleCatchUp(TerminalEquipmentPlacedInNodeContainer @event)
    {
        var newEquipment = new Equipment(
            @event.Equipment.Id,
            @event.Equipment.Name,
            @event.Equipment.SpecificationId);

        var isNewEquipmentSpecificationIndexable = _specifications.ContainsKey(newEquipment.SpecificationId);

        if (!string.IsNullOrWhiteSpace(newEquipment.Name) &&
            isNewEquipmentSpecificationIndexable)
        {
            var typesenseEquipment = new TypesenseEquipment(newEquipment.Id, newEquipment.Name);
            await _typesense.UpsertDocument(_settings.UniqueCollectionName, typesenseEquipment)
                .ConfigureAwait(false);
        }

        _equipments.Add(newEquipment.Id, newEquipment);
    }

    private async Task HandleCatchUp(TerminalEquipmentNamingInfoChanged @event)
    {
        var oldEquipment = _equipments[@event.TerminalEquipmentId];
        var updatedEquipment = oldEquipment with { Name = @event.NamingInfo?.Name };

        var isNewSpecificationIndexable = _specifications.ContainsKey(updatedEquipment.SpecificationId);

        // If it has a name and has searchable specification we update the document.
        if (isNewSpecificationIndexable)
        {
            // If name is valid, we update it.
            if (!string.IsNullOrWhiteSpace(updatedEquipment.Name))
            {
                var document = new TypesenseEquipment(updatedEquipment.Id, updatedEquipment.Name);
                await _typesense.UpdateDocument(
                    _settings.UniqueCollectionName, oldEquipment.Id.ToString(), updatedEquipment).ConfigureAwait(false);
            }
            // If name has been set to null, empty or whitespace we remove it from Typesense.
            else
            {
                await _typesense.DeleteDocument<TypesenseEquipment>(
                    _settings.UniqueCollectionName, @event.TerminalEquipmentId.ToString()).ConfigureAwait(false);
            }
        }

        _equipments[oldEquipment.Id] = updatedEquipment;
    }

    private async Task HandleCatchUp(TerminalEquipmentSpecificationChanged @event)
    {
        var oldEquipment = _equipments[@event.TerminalEquipmentId];
        var updatedEquipment = oldEquipment with { SpecificationId = @event.NewSpecificationId };

        var isOldSpecificationIndexable = _specifications.ContainsKey(oldEquipment.SpecificationId);
        var isNewSpecificationIndexable = _specifications.ContainsKey(updatedEquipment.SpecificationId);

        if (isOldSpecificationIndexable)
        {
            // If the new is not indexable we remove the indexed document. Otherwise we do nothing.
            if (!isNewSpecificationIndexable)
            {
                await _typesense.DeleteDocument<TypesenseEquipment>(
                    _settings.UniqueCollectionName, @event.TerminalEquipmentId.ToString()).ConfigureAwait(false);
            }
        }
        else
        {
            // If document has changed to be indexable we index it. Otherwise we do nothing.
            if (isNewSpecificationIndexable && !string.IsNullOrWhiteSpace(updatedEquipment.Name))
            {
                var document = new TypesenseEquipment(updatedEquipment.Id, updatedEquipment.Name);
                await _typesense.UpdateDocument(_settings.UniqueCollectionName, oldEquipment.Id.ToString(), oldEquipment)
                    .ConfigureAwait(false);
            }
        }

        _equipments[oldEquipment.Id] = updatedEquipment;
    }

    private async Task HandleCatchUp(TerminalEquipmentRemoved @event)
    {
        try
        {
            await _typesense.DeleteDocument<TypesenseEquipment>(
                _settings.UniqueCollectionName, @event.TerminalEquipmentId.ToString()).ConfigureAwait(false);
        }
        catch (TypesenseApiNotFoundException)
        {
            // It is okay, it could be removed in another case, because the name was removed.
        }

        _equipments.Remove(@event.TerminalEquipmentId);
    }
}
