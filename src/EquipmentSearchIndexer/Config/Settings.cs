using System.Collections.Generic;

namespace EquipmentSearchIndexer.Config;

internal record Settings
{
    public string CollectionAliasName { get; set; } = string.Empty;
    public List<string> SpecificationNames { get; set; } = new();
    public string UniqueCollectionName { get; set; } = string.Empty;
}
