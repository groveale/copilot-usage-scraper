using groveale.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace groveale
{
    public class GetUsersWhoHaveCompletedActivity
    {
        private readonly ILogger<GetUsersWhoHaveCompletedActivity> _logger;
        private readonly ISettingsService _settingsService;
        private readonly ICopilotUsageSnapshotService _storageSnapshotService;

        public GetUsersWhoHaveCompletedActivity(ILogger<GetUsersWhoHaveCompletedActivity> logger, ISettingsService settingsService, ICopilotUsageSnapshotService storageSnapshotService)
        {
            _logger = logger;
            _settingsService = settingsService;
            _storageSnapshotService = storageSnapshotService;
        }

        [Function("GetUsersWhoHaveCompletedActivity")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            // Get params, app, count, timeFrame, and startdate (optional)
            string app = req.Query["app"];
            string count = req.Query["count"];
            string timeFrame = req.Query["timeFrame"];
            string startDate = req.Query["startDate"];

            // also check in the body
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            app = app ?? data?.app;
            count = count ?? data?.count;
            timeFrame = timeFrame ?? data?.timeFrame;
            startDate = startDate ?? data?.startDate;
            

            // Validate params
            if (string.IsNullOrEmpty(app) || string.IsNullOrEmpty(count) || string.IsNullOrEmpty(timeFrame))
            {
                return new BadRequestObjectResult("Please pass a app, count, and timeFrame on the query string or body");
            }

            // Further validation
            if (timeFrame != "allTime" && string.IsNullOrEmpty(startDate))
            {
                return new BadRequestObjectResult("Please pass a app, count, timeFrame, and startDate on the query string or body");
            }


            // Get current start date form time frame
            var startDateForTimeFrame = await _storageSnapshotService.GetStartDate(timeFrame);
            if (startDateForTimeFrame == null)
            {
                return new BadRequestObjectResult("No date yet - wait until tomorrow");
            }

            // define an object to return
            var usersThatHaveAchieved = new List<string>();
            var startDateStatus = "Active";

            // if startDateForTimeFrame == startDate from input then we are good
            if (startDateForTimeFrame == startDate)
            {
                // Get users who have completed the activity
                var users = await _storageSnapshotService.GetUsersWhoHaveCompletedActivity(app, count, timeFrame, startDate);
                usersThatHaveAchieved = users;
            }
            else
            {
                // convert the strings into dates
                var startDateForTimeFrameDate = DateTime.Parse(startDateForTimeFrame);
                var startDateDate = DateTime.Parse(startDate);

                // work out if startDate from paramerter is before or after startDate from timeFrame, if before then return expired
                if (startDateDate < startDateForTimeFrameDate)
                {
                    startDateStatus = "Expired";
                }
                else
                {
                    startDateStatus = "Future";
                }
                
            }

            return new OkObjectResult(new { Users = usersThatHaveAchieved, StartDateStatus = startDateStatus });
        }
    }
}
