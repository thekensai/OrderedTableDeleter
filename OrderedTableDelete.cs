using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Reflection;
using System.IO;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using System.Configuration;
using System.Data.SqlClient;



class Task
{
    internal void Generate(IEnumerable<string> orderedTables)
    {
        var sb = new StringBuilder();
        var wheresb = new StringBuilder();

        var threshold = DateTime.Now.AddTicks(-this.MaxRetention.Ticks);

        var clause = this.Name == "PurgePublishedConfigRecords" ?
            "a.[IsActive] <> 1" : "a.[IsDeleted] = 1";

        foreach (var tbl in orderedTables)
        {
            if (!this.Tables.Contains(tbl)) continue;

            sb.AppendFormat("delete [{0}] from [{0}] a ", tbl);

            if (this.ParentTables.ContainsKey(tbl))
            {
                char c = 'b';
                var parents = new HashSet<string>();

                foreach (var fk in ParentTables[tbl])
                {
                    var pname = fk.Parent.Name;
                    if (parents.Contains(pname)) continue;

                    sb.AppendFormat(" left join [{0}] {1} on ", pname, c);
                    parents.Add(pname);

                    sb.Append(string.Join(" and ",
                        fk.Columns.Cast<ForeignKeyColumn>().Select(
                        col => string.Format("a.[{0}] = {1}.[{0}]", col.Name, c))));

                    wheresb.Append(" and ");
                    wheresb.Append(string.Join(" and ",
                        fk.Columns.Cast<ForeignKeyColumn>().Select(
                        col => string.Format("[{1}].[{0}] is null", col.Name, c))));

                    c++;
                }
            }

            sb.Append(" where 1=1 ");
            sb.Append(wheresb);
            wheresb.Clear();

            sb.AppendFormat(" and a.[datetimelastmodified] < @threshold and {0}\r\n\r\n", clause);
        }

        if (this.RebuildIndexes || sb.Length > 0)
            using (var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["AdminRepositoryConnectionString"].ConnectionString))
            {
                var sconn = new ServerConnection(conn);
                Server srv = new Server(sconn);
                Database db = srv.Databases["AutomationAdmin"];

                if (this.RebuildIndexes)
                    foreach (Table tbl in db.Tables)
                    {
                        if (this.Tables.Contains(tbl.Name.ToLower()))
                            tbl.RebuildIndexes(95);
                    }

                Console.WriteLine(sb.ToString());
                //if (sb.Length > 0) db.ExecuteNonQuery(sb.ToString());
            }

        this.ProcessedTime = DateTime.Now;

    }
}

    class Program
    {
        private enum AdminTaskEnum
        {
            PurgeDeletedAdminRecords,
            PurgeDeletedMasterFileRecords,
            PurgePublishedConfigRecords
        }

        static void Main(string[] args)
        {
            var taskXml = Path.Combine(Path.GetDirectoryName((typeof(Program)).Assembly.Location), "task.xml");

            if (!File.Exists(taskXml))
            {
                return;
            }

            var tasks = ReadTasks(taskXml);

            if (!tasks.Any())
            {
                //return;
            }

            var purgeDeletedAdminRecords = tasks.FirstOrDefault(t => t.Name == AdminTaskEnum.PurgeDeletedAdminRecords.ToString());
            var purgeDeletedMasterFileRecords = tasks.FirstOrDefault(t => t.Name == AdminTaskEnum.PurgeDeletedMasterFileRecords.ToString());
            var purgePublishedConfigRecords = tasks.FirstOrDefault(t => t.Name == AdminTaskEnum.PurgePublishedConfigRecords.ToString());

            if (purgeDeletedAdminRecords != null)
                new string[] {"CampaignTrackingSettings",
                "Channel",
                "CpmFloor",
                "DayPart",
                "DayPartSchedule",
                "DayPartScheduleChannel",
                "Demographic",
                "EventGrade",
                "ExclusionZone",
                "Genre",
                "InventoryFilterSet",
                "HistoricalWeekRange",
                "Market",
                "MeasureValueFilter",
                "MediaType",
                "MediaTypeOverlay",
                "MultiSpottingRule",
                "Network",
                "ProductClassification",
                "Session",
                "SplitInfo",
                "SplitTarget",
                "SubNetwork",
                "SurveyMapping",
                "Universe"}.ForEach(s => purgeDeletedAdminRecords.Tables.Add(s.ToLower()));

            if (purgeDeletedMasterFileRecords != null)
                new string[] {"Advertiser",
                "Agency",
                "Product",
                "SalesAssistant",
                "SalesRep"
            }.ForEach(s => purgeDeletedMasterFileRecords.Tables.Add(s.ToLower())); ;

            if (purgePublishedConfigRecords != null)
                purgePublishedConfigRecords.Tables.Add("PublishedConfiguration");

            var adminTasks = new Task[] { purgeDeletedAdminRecords, purgeDeletedMasterFileRecords, purgePublishedConfigRecords }
                .Where(t => t != null);

            var list = CheckAdminTasks(purgeDeletedAdminRecords, purgeDeletedMasterFileRecords, purgePublishedConfigRecords);
            
            foreach (var task in adminTasks) task.Generate(list);
        }

        
        private static IEnumerable<string> CheckAdminTasks(params Task[] tasks)
        {
            var tablesToClear = new HashSet<string>();
            foreach (var task in tasks)
                if (task != null)
                    foreach (var table in task.Tables)
                        tablesToClear.Add(table.ToLower());

            var dbTables = new HashSet<string>();
            var dictTable2Id = new Dictionary<string, int>();
            var dictId2Table = new Dictionary<int, string>();
            LinkedList<int>[] adj = null;

            using (var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["AdminRepositoryConnectionString"].ConnectionString))
            {
                var sconn = new ServerConnection(conn);
                Server srv = new Server(sconn);
                Database db = srv.Databases["AutomationAdmin"];

                int idx = 0;
                foreach (Table tbl in db.Tables)
                {
                    var name = tbl.Name.ToLower();
                    dbTables.Add(name);
                    dictTable2Id.Add(name, idx);
                    dictId2Table.Add(idx, name);
                    idx++;
                }

                adj = new LinkedList<int>[dbTables.Count];
                for (int i = 0; i < dbTables.Count; ++i)
                    adj[i] = new LinkedList<int>();

                foreach (Table tbl in db.Tables)
                    foreach (ForeignKey fk in tbl.ForeignKeys)
                    {
                        var name = tbl.Name.ToLower();
                        var reffedTable = fk.ReferencedTable.ToLower();
                        if (name != reffedTable)
                            adj[dictTable2Id[name]].AddLast(dictTable2Id[reffedTable]);

                        foreach (var task in tasks)
                        {
                            if (!task.Tables.Contains(reffedTable)) continue;
                            if (!task.ParentTables.ContainsKey(reffedTable))
                                task.ParentTables.Add(reffedTable, (new ForeignKey[] { fk }).ToList());
                            else
                                task.ParentTables[reffedTable].Add(fk);
                        }

                    }
            }

            var missingTable = tablesToClear.FirstOrDefault(t => !dbTables.Contains(t));
            if (missingTable != null)
                throw new Exception("Table " + missingTable + " not found in Admin Database");

            var descdendants = new Queue<int>();
            foreach (var tbl in dbTables)
                if (!tablesToClear.Contains(tbl))
                    descdendants.Enqueue(dictTable2Id[tbl]);
            
            while (descdendants.Any())
            {
                int next = descdendants.Dequeue();

                foreach (var tblid in adj[next])
                {
                    if (!tablesToClear.Contains(dictId2Table[next])
                        && tablesToClear.Contains(dictId2Table[tblid]))
                        throw new Exception("Cannot purge table " + dictId2Table[tblid] + " without purging table " + dictId2Table[next]);

                    descdendants.Enqueue(tblid);
                }
            }

            var stack = new Stack<int>();
            var visited = new bool[dbTables.Count];
            
            for (int i = 0; i < dbTables.Count; i++)
                DFS(i, visited, adj, stack);

            while (stack.Any())
            {
                string table = dictId2Table[stack.Pop()];
                if (tablesToClear.Contains(table))
                    yield return table;
            }
        }

        private static void DFS(int v, bool[] visited, LinkedList<int>[] adj, Stack<int> stack)
        {
            if (visited[v]) return;

            visited[v] = true;

            foreach (int idx in adj[v])
                if (!visited[idx])
                    DFS(idx, visited, adj, stack);
            
            stack.Push(v);
        }

        private static IEnumerable<Task> ReadTasks(string taskXml)
        {
            List<Task> tasks = new List<Task>();

            XmlDocument doc = new XmlDocument();
            doc.Load(taskXml);

            foreach (XmlNode node in doc.SelectNodes("/MaintenanceOperations/Operation"))
            {
                tasks.Add(new Task(node.Attributes["name"].Value,
                    node.SelectNodes("Parameters/Parameter")));
            }

            return tasks;
        }
    }


