using EquipmentSearchIndexer.Config;
using Microsoft.Extensions.Hosting;
using System.IO;
using System.Threading.Tasks;

namespace EquipmentSearchIndexer;

public class Program
{
    static async Task Main(string[] args)
    {
        var root = Directory.GetCurrentDirectory();
        var dotenv = Path.Combine(root, ".env");
        DotEnv.Load(dotenv);

        using var host = HostConfig.Configure();
        await host.StartAsync();
        await host.WaitForShutdownAsync();
    }
}
