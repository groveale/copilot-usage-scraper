

public class MonthlyUsage 
{
    // Allways a 1st on the month
    public DateTime StartDate { get; set; }
    public string UPN { get; set; }
    public string DisplayName { get; set; }
    public int CopilotAllUpActivityCount { get; set; }
    public int DailyTeamsActivityCount { get; set; }
    public int DailyCopilotChatActivityCount { get; set; }
    public int DailyOutlookActivityCount { get; set; }
    public int DailyWordActivityCount { get; set; }
    public int DailyExcelActivityCount { get; set; }
    public int DailyPowerPointActivityCount { get; set; }
    public int DailyOneNoteActivityCount { get; set; }
    public int DailyLoopActivityCount { get; set; }
}