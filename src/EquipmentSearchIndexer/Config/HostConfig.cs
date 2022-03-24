using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenFTTH.EventSourcing;
using OpenFTTH.EventSourcing.Postgres;
using Serilog;
using Serilog.Events;
using Serilog.Formatting.Compact;
using System;
using System.Collections.Generic;
using Typesense.Setup;

namespace EquipmentSearchIndexer.Config;

public static class HostConfig
{
    public static IHost Configure()
    {
        var hostBuilder = new HostBuilder();
        ConfigureApp(hostBuilder);
        ConfigureLogging(hostBuilder);
        ConfigureServices(hostBuilder);
        return hostBuilder.Build();
    }

    private static void ConfigureApp(IHostBuilder hostBuilder)
    {
        hostBuilder.ConfigureAppConfiguration((hostingContext, config) =>
        {
            config.AddEnvironmentVariables();
        });
    }

    private static void ConfigureServices(IHostBuilder hostBuilder)
    {
        hostBuilder.ConfigureServices((hostContext, services) =>
        {
            services.AddOptions();
            services.AddHostedService<EquipmentSearchIndexerHost>();
            services.Configure<Settings>(s => hostContext.Configuration.GetSection("Settings").Bind(s));
            services.AddTypesenseClient(c =>
            {
                c.ApiKey = Environment.GetEnvironmentVariable("TYPESENSE__APIKEY");
                c.Nodes = new List<Node>
                {
                    new Node
                    {
                        Host = Environment.GetEnvironmentVariable("TYPESENSE__HOST"),
                        Port = Environment.GetEnvironmentVariable("TYPESENSE__PORT"),
                        Protocol = Environment.GetEnvironmentVariable("TYPESENSE__PROTOCOL"),
                    }
                };
            });
            services.AddSingleton<IProjection, EquipmentSearchIndexerProjection>();
            services.AddSingleton<IEventStore>(
                e =>
                new PostgresEventStore(
                    serviceProvider: e.GetRequiredService<IServiceProvider>(),
                    connectionString: Environment.GetEnvironmentVariable("CONNECTIONSTRING"),
                    databaseSchemaName: "events"
                ) as IEventStore);
        });
    }

    private static void ConfigureLogging(IHostBuilder hostBuilder)
    {
        hostBuilder.ConfigureServices((hostContext, services) =>
        {
            var loggingConfiguration = new ConfigurationBuilder()
               .AddEnvironmentVariables().Build();

            services.AddLogging(loggingBuilder =>
            {
                var logger = new LoggerConfiguration()
                    .ReadFrom.Configuration(loggingConfiguration)
                    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                    .MinimumLevel.Override("System", LogEventLevel.Warning)
                    .Enrich.FromLogContext()
                    .WriteTo.Console(new CompactJsonFormatter())
                    .CreateLogger();

                loggingBuilder.AddSerilog(logger, true);
            });
        });
    }
}
