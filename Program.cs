using Google.Cloud.Spanner.Admin.Database.V1;
using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.Data;
using System;

namespace SpannerDbCleanUp
{
    class Program
    {
        private static string projectId = Environment.GetEnvironmentVariable("GOOGLE_PROJECT_ID");
        private static string instanceId =
            Environment.GetEnvironmentVariable("TEST_SPANNER_INSTANCE") ?? "my-instance";

        static void Main(string[] args)
        {
            DelelteBackups();
            DelelteDatabases();
            Console.WriteLine("Hello World!");
        }

        public static void DelelteBackups()
        {
            DatabaseAdminClient databaseAdminClient = DatabaseAdminClient.Create();

            //delete backup contains "a"
            var dataTime = DateTime.UtcNow.AddDays(1).ToString("yyyy-MM-dd");
            var listBackupRequest = new ListBackupsRequest
            {
                Parent = InstanceName.Format(projectId, instanceId),
                Filter = $"create_time < {dataTime}"
            };

            var backups = databaseAdminClient.ListBackups(listBackupRequest);
            foreach (var backup in backups)
            {
                var deleteBackupRequest = new DeleteBackupRequest()
                {
                    Name = backup.Name
                };
                databaseAdminClient.DeleteBackup(deleteBackupRequest);
            }
        }

        public static void DelelteDatabases()
        {
            string adminConnectionString = $"Data Source=projects/{projectId}/"
              + $"instances/{instanceId}";

            DatabaseAdminClient databaseAdminClient = DatabaseAdminClient.Create();
            InstanceName instanceName = InstanceName.FromProjectInstance(projectId, instanceId);
            var databases = databaseAdminClient.ListDatabases(instanceName);

            using (var connection = new SpannerConnection(adminConnectionString))
                foreach (var database in databases)
                {
                    using (var cmd = connection.CreateDdlCommand($@"DROP DATABASE {database.DatabaseName.DatabaseId}"))
                    {
                        try
                        {
                            cmd.ExecuteNonQuery();
                        }
                        catch (Exception)
                        {
                            continue;
                        }
                    }
                }
        }
    }
}
