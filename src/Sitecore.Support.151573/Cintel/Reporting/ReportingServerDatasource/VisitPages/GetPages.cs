using System;
using System.Data;
using Sitecore.Analytics.Reporting;
using Sitecore.Cintel.Reporting;
using Sitecore.Cintel.Reporting.Processors;
using Sitecore.Cintel.Reporting.ReportingServerDatasource;
using Sitecore.Configuration;
using Sitecore.Diagnostics;

namespace Sitecore.Support.Cintel.Reporting.ReportingServerDatasource.VisitPages
{
    public class GetPages : ReportProcessorBase
    {
        private const string DataSourceName = "collection";
        private static readonly QueryBuilder pagesQuery;

        static GetPages()
        {
            QueryBuilder builder = new QueryBuilder
            {
                collectionName = "Interactions"
            };
            builder.QueryParms.Add("_id", "@visitId");
            builder.Fields.Add("ContactId");
            builder.Fields.Add("_id");
            builder.Fields.Add("Keywords");
            builder.Fields.Add("StartDateTime");
            builder.Fields.Add("EndDateTime");
            builder.Fields.Add("Pages_Item__id");
            builder.Fields.Add("Pages_Url_Path");
            builder.Fields.Add("Pages_Url_QueryString");
            builder.Fields.Add("Pages_Duration");
            builder.Fields.Add("Pages_DateTime");
            builder.Fields.Add("Pages_VisitPageIndex");
            builder.Fields.Add("Pages_PageEvents_Name");
            builder.Fields.Add("Pages_PageEvents_PageEventDefinitionId");
            builder.Fields.Add("Pages_PageEvents_Value");
            builder.Fields.Add("Pages_PageEvents_DateTime");
            builder.Fields.Add("Pages_PageEvents_DataKey");

            #region patch 
            builder.Fields.Add("SiteName");
            #endregion patch

            pagesQuery = builder;
        }

        public static DataTable GetInteractions(Guid contactId)
        {
            ReportDataProvider reportDataProvider = GetReportDataProvider();
            Assert.IsNotNull(reportDataProvider, "provider should not be null");
            ReportDataQuery query = new ReportDataQuery(pagesQuery.Build())
            {
                Parameters = { {
                    "@visitId",
                    contactId
                } }
            };
            CachingPolicy cachingPolicy = new CachingPolicy
            {
                NoCache = true
            };
            return reportDataProvider.GetData("collection", query, cachingPolicy).GetDataTable();
        }

        private static ReportDataProvider GetReportDataProvider() =>
            ((ReportDataProvider)Factory.CreateObject("reporting/dataProvider", true));

        public override void Process(ReportProcessorArgs args)
        {
            Guid guid;
            if (Guid.TryParse(args.ReportParameters.ViewEntityId, out guid))
            {
                DataTable interactions = GetInteractions(guid);
                args.QueryResult = interactions;
            }
        }
    }
}
