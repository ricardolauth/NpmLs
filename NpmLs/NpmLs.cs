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
        private static readonly HttpClient HttpClient = new();

        public NpmLs(ILogger<NpmLs> logger)
        {
            _logger = logger;
        }

        [Function("npm-ls")]
        public async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            string? name = req.Query["name"];
            string? version = req.Query["version"];

            if (string.IsNullOrEmpty(name))
            {
                return new BadRequestObjectResult("Please provide both 'name' query parameters.");
            }

            var result = await LsAsync(name, version ?? "latest");
            return new OkObjectResult(result);
        }

        public class LsOptions
        {
            public readonly ConcurrentQueue<TaskItem> TaskQueue = new();
            public readonly CancellationTokenSource Cts = new();
            public readonly CountdownEvent Countdown = new(1);

        }

        private async Task<Dictionary<string, object>> LsAsync(string name, string version)
        {

            var options = new LsOptions();
            var edges = new ConcurrentBag<string>();
            var nodes = new ConcurrentBag<Metadata>();

            var initialTask = new TaskItem
            {
                Name = name,
                Version = version,
                Nodes = nodes,
                Edges = edges,
            };

            // Add dependency tasks to the channel


            var consumers = StartConsumers(64, options);  // Start 32 consumers
            options.TaskQueue.Enqueue(initialTask);


            // Wait for all tasks to complete
            options.Countdown.Wait(options.Cts.Token);
            await options.Cts.CancelAsync(); // Cancel consumers after all work is done
            try
            {
                await Task.WhenAll(consumers);
            }
            catch (TaskCanceledException) { }; // ignore cancelation exception     

            return new Dictionary<string, object>
            {
                { "nodes", nodes },
                { "edges", edges },
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
                                options.Countdown.Signal();
                            }
                            catch(Exception e)
                            {
                                options.Countdown.Signal();
                                await options.Cts.CancelAsync();
                                throw;
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
            var response = await HttpClient.GetAsync($"https://registry.npmjs.org/{couchPackageName}");
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogError("Could not load {PackageName}@{PackageVersion}", task.Name, task.Version);
                return;
                
            }

            var json = await response.Content.ReadAsStringAsync();
     
                var packageJson = JsonConvert.DeserializeObject<Package>(json);

                if (packageJson == null)
                {
                    _logger.LogError("Could not read response {PackageName}@{PackageVersion}", task.Name, task.Version);
                    return;
                }

                WalkDependenciesAsync(task, packageJson, options);
        }

        private void WalkDependenciesAsync(TaskItem task, Package packageJson, LsOptions options)
        {
            var version = GuessVersion(task.Version, packageJson);
            if(version == null)
            {
                _logger.LogError("Could not find version that satisfyes {PackageName}@{PackageVersion}", task.Name, task.Version);
                return;
            }
            var dependencies = packageJson.Versions[version].Dependencies ?? [];
            var id = $"{packageJson.Name}@{version}";
            if (task.ParentNodeId != null)
            {
                var edge = $"{task.ParentNodeId}->{id}";
                task.Edges.Add(edge);
            }

            if (task.Nodes.Select(n => n.Id).Contains(id))
            {
                return;
            }

            task.Nodes.Add(ToMetadata(packageJson, packageJson.Versions[version]));

            foreach (var dep in dependencies)
            {
                var depTask = new TaskItem
                {
                    Name = dep.Key,
                    Version = dep.Value,
                    ParentNodeId = id,
                    Nodes = task.Nodes,
                    Edges = task.Edges,
                };

                options.TaskQueue.Enqueue(depTask);
                options.Countdown.AddCount();
            }
        }

        private static string? GuessVersion(string versionString, Package packageJson)
        {
            if (versionString == "latest") versionString = "*";
            if (versionString.Contains('@'))
            {
                versionString = versionString.Split('@').Last();
            }

            var availableVersions = packageJson.Versions.Keys.ToList();
     
            var version = availableVersions.MaxSatisfying(versionString);

            if (version == null && versionString == "*" && availableVersions.All(av => SemVersion.Parse(av).Prerelease != null))
            {
                version = packageJson.DistTags.Latest;
            }

            return version;
        }

        private static Metadata ToMetadata(Package packageJson, PackageVersion version)
        {
            return new Metadata
            {
                Id = $"{packageJson.Name}@{version.Version}",
                Version = version.Version,
                Name = packageJson.Name,
                Description = packageJson.Description,
                Deprecated = version.Deprecated != null,
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
        public required string Name { get; set; }
        public required string Version { get; set; }
        public string? ParentNodeId { get; set; }
        public required ConcurrentBag<Metadata> Nodes { get; set; }
        public required ConcurrentBag<string> Edges { get; set; }
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
            try
            {
                var maxVersion = versions
                    .Select(v => SemVersion.Parse(v))
                    .Where(v => v.SatisfiesNpm(versionString.Replace("npm:", ""))); // Check if version satisfies the range



                return maxVersion?.Max(new SemverComparer())?.ToString();

            }catch(Exception e)
            {
                throw;
            }

        }
    }

    public class Package
    {
        public required string Id { get; set; }
        public string? Rev { get; set; }
        public required string Name { get; set; }
        [JsonProperty("dist-tags")]
        public required DistTags DistTags { get; set; }
        public required Dictionary<string, PackageVersion> Versions { get; set; }
        public required Dictionary<string, DateTime> Time { get; set; }
        public string? License { get; set; }
        public string? Homepage { get; set; }
        public List<string>? Keywords { get; set; }
        public object? Repository { get; set; }
        public string? Description { get; set; }
        public List<Maintainer>? Maintainers { get; set; }
        public string? Readme { get; set; }
        public string? ReadmeFilename { get; set; }
    }

    public class PackageVersion
    {
        public required string Name { get; set; }
        public required string Version { get; set; }
        public string? Id { get; set; }       
        public Dictionary<string, string>? Dependencies { get; set; }
        public Dictionary<string, string>? PeerDependencies { get; set; }
        public object? Deprecated { get; set; } 
    }

    public class Bugs
    {
        public string? Url { get; set; }
    }

    public class DistTags
    {
        public string? Latest { get; set; }
        public string? Beta { get; set; }
        public string? Experimental { get; set; }
        public string? Rc { get; set; }
        public string? Next { get; set; }
        public string? Canary { get; set; }
    }

    public class Maintainer
    {
        public required string Name { get; set; }
        public string? Email { get; set; }
    }

    public class Repository
    {
        public string? Url { get; set; }
        public string? Type { get; set; }
        public string? Directory { get; set; }
    }

    public class Metadata
    {
        public required string Id { get; set; }
        public required string Name { get; set; }
        public required string Version { get; set; }
        public string? Description { get; set; }
        public List<Maintainer>? Maintainers { get; set; }
        public object? Repository { get; set; }
        public List<string>? Keywords { get; set; }
        public string? Homepage { get; set; }
        public bool Deprecated { get; set; }
        public string? License { get; set; }
        public DistTags? DistTags { get; set; }
        public Dictionary<string, string>? PeerDependencies { get; set; }
        public Dictionary<string, DateTime>? Time { get; set; }
    }
}
