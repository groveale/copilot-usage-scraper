//using Microsoft.Graph;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using Microsoft.Graph.Beta;
using Microsoft.Graph.Beta.Models;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Tasks;

namespace groveale.Services
{
    public interface IGraphService
    {
        Task GetTodaysCopilotUsageDataAsync();
        Task<List<M365CopilotUsage>> GetM365CopilotUsageReportAsyncJSON(Microsoft.Extensions.Logging.ILogger _logger);
        Task SetReportAnonSettingsAsync(bool displayConcealedNames);
        Task<AdminReportSettings> GetReportAnonSettingsAsync();
    }

    public class GraphService : IGraphService
    {
        private readonly GraphServiceClient _graphServiceClient;
        private DefaultAzureCredential _defaultCredential;
        private ClientSecretCredential _clientSecretCredential;

        public GraphService()
        {
            //_defaultCredential = new DefaultAzureCredential();

             _clientSecretCredential = new ClientSecretCredential(
                System.Environment.GetEnvironmentVariable("AZURE_TENANT_ID"), 
                System.Environment.GetEnvironmentVariable("AZURE_CLIENT_ID"),
                System.Environment.GetEnvironmentVariable("AZURE_CLIENT_SECRET"));

            _graphServiceClient = new GraphServiceClient(_clientSecretCredential,
                // Use the default scope, which will request the scopes
                // configured on the app registration
                new[] {"https://graph.microsoft.com/.default"});
        }

        public async Task GetTodaysCopilotUsageDataAsync()
        {
            var usageData = await _graphServiceClient.Reports
                .GetMicrosoft365CopilotUsageUserDetailWithPeriod("D7")
                .GetAsync();

            // Process the usage data as needed
            Console.WriteLine(usageData.Length);
        }

        public async Task<List<M365CopilotUsage>> GetM365CopilotUsageReportAsyncJSON(Microsoft.Extensions.Logging.ILogger _logger)
        {

            var urlString = _graphServiceClient.Reports.GetMicrosoft365CopilotUsageUserDetailWithPeriod("D7").ToGetRequestInformation().URI.OriginalString;
            urlString += "?$format=application/json";//append the query parameter
            // default is top 200 rows, we can use the below to increase this
            //urlString += "?$format=application/json&$top=600";
            var m365CopilotUsageReportResponse = await _graphServiceClient.Reports.GetMicrosoft365CopilotUsageUserDetailWithPeriod("D7").WithUrl(urlString).GetAsync();

            byte[] buffer = new byte[8192];
            int bytesRead;
            List<M365CopilotUsage> m365CopilotUsageReports = new List<M365CopilotUsage>();

            do {

                string usageReportsInChunk = "";

                while ((bytesRead = await m365CopilotUsageReportResponse.ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    // Process the chunk of data here
                    string chunk = System.Text.Encoding.UTF8.GetString(buffer, 0, bytesRead);
                    usageReportsInChunk += chunk;
                }

                using (JsonDocument doc = JsonDocument.Parse(usageReportsInChunk))
                {
                    // Append the site data to the site dataString
                    if (doc.RootElement.TryGetProperty("value", out JsonElement usageReports))
                    {
                        var reports = JsonSerializer.Deserialize<List<M365CopilotUsage>>(usageReports.GetRawText());
                        m365CopilotUsageReports.AddRange(reports);
                        _logger.LogInformation($"Total User reports: {m365CopilotUsageReports.Count}");

                    }

                    if (doc.RootElement.TryGetProperty("@odata.nextLink", out JsonElement nextLinkElement))
                    {
                        urlString = nextLinkElement.GetString(); 
                    }
                    else
                    {
                        urlString = null; // No more pages break out of the loop
                        break;
                    }
                }

                m365CopilotUsageReportResponse = await _graphServiceClient.Reports.GetMicrosoft365CopilotUsageUserDetailWithPeriod("D7").WithUrl(urlString).GetAsync();

            } while (urlString != null);


            return m365CopilotUsageReports;
        }

        public async Task SetReportAnonSettingsAsync(bool displayConcealedNames)
        {
            var adminReportSettings = new AdminReportSettings
            {
                DisplayConcealedNames = displayConcealedNames
            };

            var result = await _graphServiceClient.Admin.ReportSettings.PatchAsync(adminReportSettings);
        }

        public async Task<AdminReportSettings> GetReportAnonSettingsAsync()
        {
            var result = await _graphServiceClient.Admin.ReportSettings.GetAsync();
            return result;
        }
    }

}