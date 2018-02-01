using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Sitecore.Cintel.Commons;
using Sitecore.Cintel.Reporting;
using Sitecore.Cintel.Reporting.Contact.Page;
using Sitecore.Cintel.Reporting.Processors;
using Sitecore.Cintel.Reporting.Utility;
using Sitecore.Diagnostics;
using Sitecore.Globalization;
using Sitecore.Sites;

namespace Sitecore.Support.Cintel.Reporting.Contact.Page.Processors
{
    public class PopulateVisitPagesWithXdbData : ReportProcessorBase
    {
        private static Dictionary<int, int> CalculatePageEvents(DataTable rawTable) =>
            (from row in rawTable.AsEnumerable()
             group row by row.Field<int>("Pages_VisitPageIndex") into grp
             select new
             {
                 PageIndex = grp.Key,
                 Value = grp.Sum<DataRow>((Func<DataRow, int>)(g => g.Field<int?>("Pages_PageEvents_Value").GetValueOrDefault()))
             }).ToDictionary(e => e.PageIndex, e => e.Value);

        private static Dictionary<int, string> GroupPageEvents(DataTable rawTable) =>
            (from row in rawTable.AsEnumerable()
             group row by row.Field<int>("Pages_VisitPageIndex") into grp
             select new
             {
                 PageIndex = grp.Key,
                 Value = string.Join(", ", (IEnumerable<string>)(from g in grp
                                                                 where ReportProcessorBase.ShouldPageEventBeVisible(g.Field<Guid?>("Pages_PageEvents_PageEventDefinitionId"), EventViewType.PageEvents)
                                                                 select ReportProcessorBase.GetDisplayName(g.Field<Guid>("Pages_PageEvents_PageEventDefinitionId"))))
             }).ToDictionary(e => e.PageIndex, e => e.Value);

        public override void Process(ReportProcessorArgs args)
        {
            DataTable queryResult = args.QueryResult;
            DataTable resultTableForView = args.ResultTableForView;
            Dictionary<int, string> dictionary = GroupPageEvents(queryResult);
            Dictionary<int, int> dictionary2 = CalculatePageEvents(queryResult);
            using (Dictionary<int, string>.Enumerator enumerator = dictionary.GetEnumerator())
            {
                while (enumerator.MoveNext())
                {
                    Func<DataRow, bool> predicate = null;
                    KeyValuePair<int, string> keyValue = enumerator.Current;
                    if (predicate == null)
                    {
                        predicate = r => r["Pages_VisitPageIndex"].Equals(keyValue.Key);
                    }
                    DataRow sourceRow = queryResult.AsEnumerable().First<DataRow>(predicate);
                    DataRow targetRow = resultTableForView.NewRow();
                    if ((!base.TryFillData<Guid>(targetRow, Schema.ContactId, sourceRow, "ContactId") || !base.TryFillData<Guid>(targetRow, Schema.VisitId, sourceRow, "_id")) || !base.TryFillData<string>(targetRow, Schema.Url, sourceRow, "Pages_Url_Path"))
                    {
                        ReportProcessorBase.LogNotificationForView(args.ReportParameters.ViewName, new NotificationMessage
                        {
                            Id = 13,
                            MessageType = NotificationTypes.Error,
                            Text = Translate.Text("One or more data entries are missing due to invalid data")
                        });
                    }
                    else 
                    {
#region patch
                        if (base.TryFillData<string>(targetRow, new ViewField<string>("SiteName"), sourceRow, "SiteName"))
                        {
                            Assert.IsNotNull(targetRow["SiteName"], "targetRow['SiteName'] != null");
                            var site = SiteContext.GetSite(targetRow["SiteName"].ToString());
                            Assert.IsNotNull(targetRow["SiteName"], "site != null");
                            targetRow["SiteName"] = !String.IsNullOrEmpty(site.TargetHostName) ? site.TargetHostName : site.HostName;
                        }
                        else
                        {
                            targetRow["SiteName"] = "";
                        }                       
#endregion patch

                        bool flag2 = ((!base.TryFillData<DateTime>(targetRow, Schema.PageStartDateTime, sourceRow, "Pages_DateTime") || !base.TryFillData<Guid>(targetRow, Schema.ItemId, sourceRow, "Pages_Item__id")) || !base.TryFillPagePathAndQuery<string>(targetRow, Schema.Url, sourceRow)) || !base.TryFillData<int>(targetRow, Schema.PageDuration, sourceRow, "Pages_Duration");
                        targetRow[Schema.PageValue.Name] = dictionary2[keyValue.Key];
                        if (targetRow.Field<int?>(Schema.PageDuration.Name).GetValueOrDefault() < 0)
                        {
                            targetRow[Schema.PageDuration.Name] = 0;
                        }
                        targetRow[Schema.PageEvents.Name] = keyValue.Value;
                        if (flag2)
                        {
                            ReportProcessorBase.LogNotificationForView(args.ReportParameters.ViewName, new NotificationMessage
                            {
                                Id = 0x13a,
                                MessageType = NotificationTypes.Warning,
                                Text = Translate.Text("Some columns may be missing data")
                            });
                        }
                        resultTableForView.Rows.Add(targetRow);
                    }
                }
            }
        }
    }
}
