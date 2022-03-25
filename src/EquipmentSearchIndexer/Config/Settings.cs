using System.Collections.Generic;

namespace EquipmentSearchIndexer.Config;

internal record Settings
{
    public List<string> SpecificationNames = new();
    public string UniqueCollectionName { get; set; } = string.Empty;
}
