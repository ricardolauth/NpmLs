using System.Collections.Concurrent;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Semver;

namespace NpmLs
{
    public class NpmLs
    {
        private readonly ILogger<NpmLs> _logger;
        private static readonly HttpClient HttpClient = new HttpClient();
        private const string RegistryUrl = "https://registry.npmjs.org";

        public NpmLs(ILogger<NpmLs> logger)
        {
            _logger = logger;
        }

        [Function("npm-ls")]
        public async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            string name = req.Query["name"];
            string version = req.Query["version"];

            if (string.IsNullOrEmpty(name))
            {
                return new BadRequestObjectResult("Please provide both 'name' query parameters.");
            }

            var result = await LsAsync(name, version ?? "latest");
            return new OkObjectResult(result);
        }

        public class LsOptions
        {
            public ConcurrentQueue<TaskItem> TaskQueue = new();
            public CancellationTokenSource Cts = new();
            public CountdownEvent Countdown = new(1);

        }

        private async Task<Dictionary<string, object>> LsAsync(string name, string version)
        {

            var options = new LsOptions();
            var nodes = new ConcurrentBag<string>();
            var edges = new ConcurrentBag<string>();
            var flat = new ConcurrentBag<Metadata>();

            var initialTask = new TaskItem
            {
                Name = name,
                Version = version,
                Nodes = nodes,
                Edges = edges,
                Flat = flat,
            };

            // Add dependency tasks to the channel


            StartConsumers(64, options);  // Start 32 consumers
            options.TaskQueue.Enqueue(initialTask);


            // Wait for all tasks to complete
            options.Countdown.Wait();
            await options.Cts.CancelAsync(); // Cancel consumers after all work is done

            return new Dictionary<string, object>
            {
                { "nodes", nodes },
                { "edges", edges },
                { "flat", flat }
            };
        }

        private IEnumerable<Task> StartConsumers(int consumerCount, LsOptions options)
        {
            var tasks = new List<Task>();

            for (int i = 0; i < consumerCount; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    while (!options.Cts.Token.IsCancellationRequested)
                    {
                        if (options.TaskQueue.TryDequeue(out var task))
                        {
                            try
                            {
                                await ProcessTaskAsync(task, options);
                            }
                            finally
                            {
                                options.Countdown.Signal();
                            }

                        }
                        else
                        {
                            // Delay to prevent busy-waiting
                            await Task.Delay(50, options.Cts.Token);
                        }
                    }
                }, options.Cts.Token));
            }

            return tasks;
        }

        private async Task ProcessTaskAsync(TaskItem task, LsOptions options)
        {
            var couchPackageName = task.Name.Replace("/", "%2F");
            var response = await HttpClient.GetAsync($"{RegistryUrl}/{couchPackageName}");
            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine($"Could not load {task.Name}@{task.Version}");
                return;
            }

            var packageJson = JsonConvert.DeserializeObject<Package>(await response.Content.ReadAsStringAsync());

            WalkDependenciesAsync(task, packageJson, options);
        }

        private static void WalkDependenciesAsync(TaskItem task, Package packageJson, LsOptions options)
        {
            var version = GuessVersion(task.Version, packageJson);
            var dependencies = packageJson.Versions[version].Dependencies ?? [];
            var id = $"{packageJson.Name}@{version}";
            if (task.ParentNode != null)
            {
                var edge = $"{task.ParentNode}->{id}";
                task.Edges.Add(edge);
            }

            if (task.Nodes.Contains(id))
            {
                return;
            }

            task.Nodes.Add(id);
            task.Flat.Add(ToMetadata(packageJson, packageJson.Versions[version]));

            foreach (var dep in dependencies)
            {
                var depTask = new TaskItem
                {
                    Name = dep.Key,
                    Version = dep.Value,
                    ParentNode = id,
                    Nodes = task.Nodes,
                    Edges = task.Edges,
                    Flat = task.Flat
                };

                options.TaskQueue.Enqueue(depTask);
                options.Countdown.AddCount();
            }
        }

        private static string GuessVersion(string versionString, Package packageJson)
        {
            if (versionString == "latest") versionString = "*";

            var availableVersions = packageJson.Versions.Keys.ToList();
            var version = availableVersions.MaxSatisfying(versionString);

            if (version == null && versionString == "*" && availableVersions.All(av => SemVersion.Parse(av).Prerelease != null))
            {
                version = packageJson.DistTags.Latest;
            }

            if (version == null)
            {
                throw new Exception($"Could not find a satisfactory version for {versionString}");
            }

            return version;
        }

        private static Metadata ToMetadata(Package packageJson, PackageVersion version)
        {
            return new Metadata
            {
                Id = $"{packageJson.Name}@{version}",
                Version = version.Version,
                Name = packageJson.Name,
                Description = packageJson.Description,
                Deprecated = version.Deprecated,
                DistTags = packageJson.DistTags,
                Keywords = packageJson.Keywords,
                License = packageJson.License,
                Maintainers = packageJson.Maintainers,
                Repository = packageJson.Repository,
                Homepage = packageJson.Homepage,
                Time = packageJson.Time            
            };
        }
    }



    public class TaskItem
    {
        public string Name { get; set; }
        public string Version { get; set; }
        public string? ParentNode { get; set; }
        public ConcurrentBag<string> Nodes { get; set; }
        public ConcurrentBag<string> Edges { get; set; }
        public ConcurrentBag<Metadata> Flat { get; set; }
    }


    public class SemverComparer : IComparer<SemVersion>
    {
        public int Compare(SemVersion? x, SemVersion? y)
        {
            return x?.CompareSortOrderTo(y) ?? 0;
        }
    }

    public static class Extensions
    {
        public static string? MaxSatisfying(this List<string> versions, string versionString)
        {
            // Filter and find the maximum satisfying version
            var maxVersion = versions
                .Select(v => SemVersion.Parse(v))
                .Where(v => v.SatisfiesNpm(versionString.Replace("npm:", ""))); // Check if version satisfies the range



            return maxVersion?.Max(new SemverComparer())?.ToString();
        }
    }



    public class Package
    {
        public string Id { get; set; }
        public string Rev { get; set; }
        public string Name { get; set; }
        public DistTags DistTags { get; set; }
        public Dictionary<string, PackageVersion> Versions { get; set; }
        public Dictionary<string, DateTime> Time { get; set; }
        public Bugs Bugs { get; set; }
        public string License { get; set; }
        public string Homepage { get; set; }
        public List<string> Keywords { get; set; }
        public Repository Repository { get; set; }
        public string Description { get; set; }
        public List<Maintainer> Maintainers { get; set; }
        public string Readme { get; set; }
        public string ReadmeFilename { get; set; }
        public Dictionary<string, bool> Users { get; set; }
    }

    public class PackageVersion
    {
        public string Name { get; set; }
        public string Version { get; set; }
        public Maintainer Author { get; set; }
        public string Id { get; set; }
        public List<Maintainer> Maintainers { get; set; }
        public Bugs Bugs { get; set; }
        public Maintainer NpmUser { get; set; }
        public List<string> Licenses { get; set; }
        public Repository Repository { get; set; }
        public string NpmVersion { get; set; }
        public string Description { get; set; }
        public string NodeVersion { get; set; }
        public Dictionary<string, string> Dependencies { get; set; }
        public Dictionary<string, string> DevDependencies { get; set; }
        public Dictionary<string, string> PeerDependencies { get; set; }
        public List<string> Keywords { get; set; }
        public object Deprecated { get; set; }
        public string Homepage { get; set; }
        public string? License { get; set; }
        public bool? BundleDependencies { get; set; }
        public string GitHead { get; set; }
    }

    public class Bugs
    {
        public string Url { get; set; }
    }

    public class DistTags
    {
        public string Latest { get; set; }
        public string Beta { get; set; }
        public string Experimental { get; set; }
        public string Rc { get; set; }
        public string Next { get; set; }
        public string Canary { get; set; }
    }

    public class Maintainer
    {
        public string Name { get; set; }
        public string Email { get; set; }
    }

    public class Repository
    {
        public string Url { get; set; }
        public string Type { get; set; }
        public string Directory { get; set; }
    }

    public class Metadata
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Version { get; set; }
        public string Description { get; set; }
        public List<Maintainer> Maintainers { get; set; }
        public Repository Repository { get; set; }
        public List<string> Keywords { get; set; }
        public string Homepage { get; set; }
        public object Deprecated { get; set; }
        public string? License { get; set; }
        public DistTags DistTags { get; set; }
        public Dictionary<string, string> PeerDependencies { get; set; }
        public Dictionary<string, DateTime> Time { get; set; }
    }


}
