using groveale;
using groveale.Services;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = FunctionsApplication.CreateBuilder(args);

builder.ConfigureFunctionsWebApplication();

// Application Insights isn't enabled by default. See https://aka.ms/AAt8mw4.
// builder.Services
//     .AddApplicationInsightsTelemetryWorkerService()
//     .ConfigureFunctionsApplicationInsights();


builder.Services.AddSingleton<IGraphService, GraphService>();
builder.Services.AddSingleton<ISettingsService, SettingsService>();
builder.Services.AddSingleton<ICopilotUsageSnapshotService, CopilotUsageSnapshotService>();
builder.Services.AddSingleton<IQueueService, QueueService>();
builder.Services.AddSingleton<IUserActivitySeeder, UserActivitySeeder>();
builder.Services.AddSingleton<IKeyVaultService, KeyVaultService>();

// For the auth hack
builder.Services.AddSingleton<ITokenService, TokenService>();
builder.Services.AddSingleton<IGraphDelegatedService, GraphDelegatedService>();

builder.Build().Run();
