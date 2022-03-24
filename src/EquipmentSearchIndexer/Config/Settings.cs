using System;

namespace EquipmentSearchIndexer.Config;

internal record Settings
{
    public string UniqueCollectionName = $"equipments-{Guid.NewGuid()}";
}
