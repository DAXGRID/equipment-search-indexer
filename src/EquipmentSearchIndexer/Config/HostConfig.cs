using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
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
            services.PostConfigure<Settings>(settings =>
            {
                var s = GetSettings();
                settings.CollectionAliasName = s.CollectionAliasName;
                settings.SpecificationNames = s.SpecificationNames;
                settings.UniqueCollectionName = s.UniqueCollectionName;
            });
            services.AddHostedService<EquipmentSearchIndexerHost>();
            services.AddTypesenseClient(c =>
            {
                c.ApiKey = GetEnvironmentVariableNotNull("TYPESENSE_APIKEY");
                c.Nodes = new List<Node>
                {
                    new Node
                    (
                        GetEnvironmentVariableNotNull("TYPESENSE_HOST"),
                        GetEnvironmentVariableNotNull("TYPESENSE_PORT"),
                        GetEnvironmentVariableNotNull("TYPESENSE_PROTOCOL")
                    )
                };
            });
            services.AddSingleton<IProjection, EquipmentSearchIndexerProjection>();
            services.AddSingleton<IEventStore>(
                e =>
                new PostgresEventStore(
                    serviceProvider: e.GetRequiredService<IServiceProvider>(),
                    connectionString: GetEnvironmentVariableNotNull("CONNECTIONSTRING"),
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

    private static Settings GetSettings()
    {
        var collectionAliasName = GetEnvironmentVariableNotNull("TYPESENSE_COLLECTION_ALIAS");
        var specificationNames = JsonConvert.DeserializeObject<List<string>>(
            GetEnvironmentVariableNotNull("SPECIFICATION_NAMES")) ??
            throw new ArgumentNullException("SPECIFICATION_NAMES");
        var uniqueCollectionName = $"{collectionAliasName}-{Guid.NewGuid()}";

        return new Settings
        {
            CollectionAliasName = collectionAliasName,
            SpecificationNames = specificationNames,
            UniqueCollectionName = uniqueCollectionName
        };
    }

    private static string GetEnvironmentVariableNotNull(string name)
        => Environment.GetEnvironmentVariable(name) ?? throw new ArgumentNullException(name);
}
