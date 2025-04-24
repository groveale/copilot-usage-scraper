
// Valid for both Weekly and monthly
using Azure.Data.Tables;
using groveale.Models;

public class TimeFrameUsage : BaseTableEntity
{
    // Allways a 1st on the month
    public DateTime StartDate { get; set; }
    public string UPN { get; set; }
    public int DailyAllActivityCount { get; set; }
    public int DailyTeamsActivityCount { get; set; }
    public int DailyCopilotChatActivityCount { get; set; }
    public int DailyOutlookActivityCount { get; set; }
    public int DailyWordActivityCount { get; set; }
    public int DailyExcelActivityCount { get; set; }
    public int DailyPowerPointActivityCount { get; set; }
    public int DailyOneNoteActivityCount { get; set; }
    public int DailyLoopActivityCount { get; set; }

     // Additional Activity Counts
    public int DailyMACActivityCount { get; set; }
    public int DailyDesignerActivityCount { get; set; }
    public int DailySharePointActivityCount { get; set; }
    public int DailyPlannerActivityCount { get; set; }
    public int DailyWhiteboardActivityCount { get; set; }
    public int DailyStreamActivityCount { get; set; }
    public int DailyFormsActivityCount { get; set; }
    public int DailyCopilotActionActivityCount { get; set; }
    public int DailyWebPluginActivityCount { get; set; }

    // Interaction Counts
    public int TeamsInteractionCount { get; set; }
    public int CopilotChatInteractionCount { get; set; }
    public int OutlookInteractionCount { get; set; }
    public int WordInteractionCount { get; set; }
    public int ExcelInteractionCount { get; set; }
    public int PowerPointInteractionCount { get; set; }
    public int OneNoteInteractionCount { get; set; }
    public int LoopInteractionCount { get; set; }
    public int MACInteractionCount { get; set; }
    public int DesignerInteractionCount { get; set; }
    public int SharePointInteractionCount { get; set; }
    public int PlannerInteractionCount { get; set; }
    public int WhiteboardInteractionCount { get; set; }
    public int StreamInteractionCount { get; set; }
    public int FormsInteractionCount { get; set; }
    public int CopilotActionInteractionCount { get; set; }
    public int WebPluginInteractionCount { get; set; }
    public int AllInteractionCount { get; set; }

    public TableEntity ToTableEntity()
    {
        PartitionKey = StartDate.ToString("yyyy-MM-dd");
        RowKey = UPN;

        return new TableEntity(PartitionKey, RowKey)
        {
            { nameof(DailyAllActivityCount), DailyAllActivityCount },
            { nameof(DailyTeamsActivityCount), DailyTeamsActivityCount },
            { nameof(DailyCopilotChatActivityCount), DailyCopilotChatActivityCount },
            { nameof(DailyOutlookActivityCount), DailyOutlookActivityCount },
            { nameof(DailyWordActivityCount), DailyWordActivityCount },
            { nameof(DailyExcelActivityCount), DailyExcelActivityCount },
            { nameof(DailyPowerPointActivityCount), DailyPowerPointActivityCount },
            { nameof(DailyOneNoteActivityCount), DailyOneNoteActivityCount },
            { nameof(DailyLoopActivityCount), DailyLoopActivityCount },
            { nameof(DailyMACActivityCount), DailyMACActivityCount },
            { nameof(DailyDesignerActivityCount), DailyDesignerActivityCount },
            { nameof(DailySharePointActivityCount), DailySharePointActivityCount },
            { nameof(DailyPlannerActivityCount), DailyPlannerActivityCount },
            { nameof(DailyWhiteboardActivityCount), DailyWhiteboardActivityCount },
            { nameof(DailyStreamActivityCount), DailyStreamActivityCount },
            { nameof(DailyFormsActivityCount), DailyFormsActivityCount },
            { nameof(DailyCopilotActionActivityCount), DailyCopilotActionActivityCount },
            { nameof(DailyWebPluginActivityCount), DailyWebPluginActivityCount },
            { nameof(TeamsInteractionCount), TeamsInteractionCount },
            { nameof(CopilotChatInteractionCount), CopilotChatInteractionCount },
            { nameof(OutlookInteractionCount), OutlookInteractionCount },
            { nameof(WordInteractionCount), WordInteractionCount },
            { nameof(ExcelInteractionCount), ExcelInteractionCount },
            { nameof(PowerPointInteractionCount), PowerPointInteractionCount },
            { nameof(OneNoteInteractionCount), OneNoteInteractionCount },
            { nameof(LoopInteractionCount), LoopInteractionCount },
            { nameof(MACInteractionCount), MACInteractionCount },
            { nameof(DesignerInteractionCount), DesignerInteractionCount },
            { nameof(SharePointInteractionCount), SharePointInteractionCount },
            { nameof(PlannerInteractionCount), PlannerInteractionCount },
            { nameof(WhiteboardInteractionCount), WhiteboardInteractionCount },
            { nameof(StreamInteractionCount), StreamInteractionCount },
            { nameof(FormsInteractionCount), FormsInteractionCount },
            { nameof(CopilotActionInteractionCount), CopilotActionInteractionCount },
            { nameof(WebPluginInteractionCount), WebPluginInteractionCount },
            { nameof(AllInteractionCount), AllInteractionCount }
        };
    }
}