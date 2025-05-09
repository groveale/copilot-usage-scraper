using Azure.Data.Tables;
using groveale.Models;

public class AllTimeUsage : BaseTableEntity
{
    public string UPN { get; set; }
    // CopilotAllUp / CopilotChat / Teams / Outlook / Word / Excel / PowerPoint / OneNote / Loop
    public AppType App {get; set; }
    // Copilot all up
    public int DailyAllTimeActivityCount { get; set; }
    public int BestDailyStreak { get; set; }
    public int CurrentDailyStreak { get; set; }
    public int AllTimeInteractionCount { get; set; }

    public TableEntity ToTableEntity()
    {
        PartitionKey = UPN;
        RowKey = App.ToString();

        return new TableEntity(PartitionKey, RowKey)
        {
            { nameof(DailyAllTimeActivityCount), DailyAllTimeActivityCount },
            { nameof(CurrentDailyStreak), CurrentDailyStreak },
            { nameof(BestDailyStreak), BestDailyStreak },
            { nameof(AllTimeInteractionCount), AllTimeInteractionCount }
        };
    }

    public string GetAppString()
    {
        return App.ToString();
    }
}

public enum AppType
{
    All,
    CopilotChat,
    Teams,
    Outlook,
    Word,
    Excel,
    PowerPoint,
    OneNote,
    Loop,
    MAC,
    Designer,
    SharePoint,
    Planner,
    Whiteboard,
    Stream,
    Forms,
    CopilotAction,
    WebPlugin
}

