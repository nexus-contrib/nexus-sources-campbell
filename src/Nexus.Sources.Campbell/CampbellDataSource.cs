using CampbellDAT;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Nexus.Sources.Campbell
{
    [ExtensionIdentification("Campbell", "Campbell", "Provides access to databases with Campbell DAT files.")]
    public class CampbellDataSource : StructuredFileDataSource
    {
        #region Fields

        private Dictionary<string, CatalogDescription> _config = null!;

        #endregion

        #region Properties

        private DataSourceContext Context { get; set; } = null!;

        #endregion

        #region Methods

        protected override async Task SetContextAsync(DataSourceContext context, CancellationToken cancellationToken)
        {
            this.Context = context;

            var configFilePath = Path.Combine(this.Root, "config.json");

            if (!File.Exists(configFilePath))
                throw new Exception($"Configuration file {configFilePath} not found.");

            var jsonString = await File.ReadAllTextAsync(configFilePath);
            _config = JsonSerializer.Deserialize<Dictionary<string, CatalogDescription>>(jsonString) ?? throw new Exception("config is null");
        }

        protected override Task<FileSourceProvider> GetFileSourceProviderAsync(CancellationToken cancellationToken)
        {
            var allFileSources = _config.ToDictionary(
                config => config.Key,
                config => config.Value.FileSources.Cast<FileSource>().ToArray());

            var fileSourceProvider = new FileSourceProvider(
                All: allFileSources,
                Single: catalogItem =>
                {
                    var properties = catalogItem.Resource.Properties;

                    if (properties is null)
                        throw new ArgumentNullException(nameof(properties));

                    return allFileSources[catalogItem.Catalog.Id]
                        .First(fileSource => ((ExtendedFileSource)fileSource).Name == properties["FileSource"]);
                });

            return Task.FromResult(fileSourceProvider);
        }

        protected override Task<string[]> GetCatalogIdsAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(_config.Keys.ToArray());
        }

        protected override Task<ResourceCatalog> GetCatalogAsync(string catalogId, CancellationToken cancellationToken)
        {
            var catalogDescription = _config[catalogId];
            var catalog = new ResourceCatalog(id: catalogId);

            foreach (var fileSource in catalogDescription.FileSources)
            {
                var filePaths = default(string[]);

                if (fileSource.CatalogSourceFiles is not null)
                {
                    filePaths = fileSource.CatalogSourceFiles
                         .Select(filePath => Path.Combine(this.Root, filePath))
                         .ToArray();
                }
                else
                {
                    if (!this.TryGetFirstFile(fileSource, out var filePath))
                        continue;

                    filePaths = new[] { filePath };
                }

                cancellationToken.ThrowIfCancellationRequested();

                foreach (var filePath in filePaths)
                {
                    var newCatalogBuilder = new ResourceCatalogBuilder(id: catalogId);

                    using var campbellFile = new CampbellFile(filePath);

                    foreach (var campbellVariable in campbellFile.Variables)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        try
                        {
                            var customParameters = fileSource.CustomParameters;

                            if (customParameters is null)
                                throw new ArgumentNullException(nameof(customParameters));

                            var samplePeriod = TimeSpan.Parse(customParameters["SamplePeriod"]);

                            var representation = new Representation(
                                dataType: Utilities.GetNexusDataTypeFromType(campbellVariable.DataType),
                                samplePeriod: samplePeriod);

                            var id = campbellVariable.Name;

                            if (customParameters.TryGetValue("ReplacePattern", out var pattern) &&
                                customParameters.TryGetValue("ReplaceValue", out var replacement))
                                id = Regex.Replace(id, pattern, replacement);

                            var resource = new ResourceBuilder(id: id)
                                .WithUnit(campbellVariable.Unit)
                                .WithGroups(fileSource.Name)
                                .WithProperty("FileSource", fileSource.Name)
                                .AddRepresentation(representation)
                                .Build();

                            newCatalogBuilder.AddResource(resource);
                        }
                        catch (NotSupportedException) // for invalid data type
                        {
                            // skip
                        }
                    }

                    catalog = catalog.Merge(newCatalogBuilder.Build(), MergeMode.NewWins);
                }
            }

            return Task.FromResult(catalog);
        }

        protected override Task ReadSingleAsync(ReadInfo info, CancellationToken cancellationToken)
        {
            return Task.Run(async () =>
            {
                using var campbellFile = new CampbellFile(info.FilePath);

                var fileSourceProvider = await this.GetFileSourceProviderAsync(cancellationToken);
                var fileSource = (ExtendedFileSource)fileSourceProvider.Single(info.CatalogItem);

                var campbellVariable = campbellFile.Variables.First(current =>
                {
                    var id = current.Name;
                    var customParameters = fileSource.CustomParameters;

                    if (customParameters is null)
                        throw new ArgumentNullException(nameof(customParameters));

                    if (customParameters.TryGetValue("ReplacePattern", out var pattern) &&
                        customParameters.TryGetValue("ReplaceValue", out var replacement))
                        id = Regex.Replace(id, pattern, replacement);

                    return id == info.CatalogItem.Resource.Id;
                });

                var campbellData = campbellFile.Read<byte>(campbellVariable);
                var result = campbellData.Data.Buffer;
                var elementSize = info.CatalogItem.Representation.ElementSize;

                cancellationToken.ThrowIfCancellationRequested();

                // write data
                if (result.Length == info.FileLength * elementSize)
                {
                    var offset = (int)info.FileOffset * elementSize;

                    result
                        .AsMemory()
                        .Slice(offset)
                        .CopyTo(info.Data);

                    info
                        .Status
                        .Span
                        .Fill(1);
                }
                // skip data
                else
                {
                    this.Context.Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                }
            });
        }

        #endregion
    }
}
