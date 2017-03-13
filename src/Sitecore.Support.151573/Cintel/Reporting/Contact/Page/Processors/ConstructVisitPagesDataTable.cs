using Sitecore.Cintel.Reporting;

namespace Sitecore.Support.Cintel.Reporting.Contact.Page.Processors
{
    public class ConstructVisitPagesDataTable : Sitecore.Cintel.Reporting.Contact.Page.Processors.ConstructVisitPagesDataTable
    {
        public override void Process(ReportProcessorArgs args)
        {
            base.Process(args);
            args.ResultTableForView.Columns.Add(new ViewField<string>("SiteName").ToColumn());
        }
    }
}
