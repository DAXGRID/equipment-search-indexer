using EquipmentSearchIndexer.Config;
using Microsoft.Extensions.Hosting;
using System.Threading.Tasks;

namespace EquipmentSearchIndexer;

public class Program
{
    static async Task Main(string[] args)
    {
        using var host = HostConfig.Configure();
        await host.StartAsync();
        await host.WaitForShutdownAsync();
    }
}
