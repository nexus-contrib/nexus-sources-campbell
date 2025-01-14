﻿using CampbellDAT;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace Nexus.Sources;

[ExtensionDescription(
    "Provides access to databases with Campbell DAT files.",
    "https://github.com/Apollo3zehn/nexus-sources-campbell",
    "https://github.com/Apollo3zehn/nexus-sources-campbell")]
public class Campbell : StructuredFileDataSource
{
    record CatalogDescription(
        string Title,
        Dictionary<string, IReadOnlyList<FileSource>> FileSourceGroups,
        JsonElement? AdditionalProperties);

    private Dictionary<string, CatalogDescription> _config = default!;

    protected override async Task InitializeAsync(CancellationToken cancellationToken)
    {
        var configFilePath = Path.Combine(Root, "config.json");

        if (!File.Exists(configFilePath))
            throw new Exception($"Configuration file {configFilePath} not found.");

        var jsonString = await File.ReadAllTextAsync(configFilePath, cancellationToken);
        _config = JsonSerializer.Deserialize<Dictionary<string, CatalogDescription>>(jsonString) ?? throw new Exception("config is null");
    }

    protected override Task<Func<string, Dictionary<string, IReadOnlyList<FileSource>>>> GetFileSourceProviderAsync(
        CancellationToken cancellationToken)
    {
        return Task.FromResult<Func<string, Dictionary<string, IReadOnlyList<FileSource>>>>(
            catalogId => _config[catalogId].FileSourceGroups);
    }

    protected override Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(string path, CancellationToken cancellationToken)
    {
        if (path == "/")
            return Task.FromResult(_config.Select(entry => new CatalogRegistration(entry.Key, entry.Value.Title)).ToArray());

        else
            return Task.FromResult(Array.Empty<CatalogRegistration>());
    }

    protected override Task<ResourceCatalog> EnrichCatalogAsync(ResourceCatalog catalog, CancellationToken cancellationToken)
    {
        var catalogDescription = _config[catalog.Id];

        foreach (var (fileSourceId, fileSourceGroup) in catalogDescription.FileSourceGroups)
        {
            foreach (var fileSource in fileSourceGroup)
            {
                var filePaths = default(string[]);

                var catalogSourceFiles = fileSource.AdditionalProperties?.GetStringArray("CatalogSourceFiles");

                if (catalogSourceFiles is not null)
                {
                    filePaths = catalogSourceFiles
                        .Where(filePath => filePath is not null)
                        .Select(filePath => Path.Combine(Root, filePath!))
                        .ToArray();
                }
                else
                {
                    if (!TryGetFirstFile(fileSource, out var filePath))
                        continue;

                    filePaths = [filePath];
                }

                cancellationToken.ThrowIfCancellationRequested();

                foreach (var filePath in filePaths)
                {
                    var newCatalogBuilder = new ResourceCatalogBuilder(id: catalog.Id);

                    using var campbellFile = new CampbellFile(filePath);

                    foreach (var campbellVariable in campbellFile.Variables)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        try
                        {
                            var additionalProperties = fileSource.AdditionalProperties;
                            var samplePeriodString = (additionalProperties?.GetStringValue("SamplePeriod")) ?? throw new Exception("The configuration parameter SamplePeriod is required.");
                            var samplePeriod = TimeSpan.Parse(samplePeriodString);

                            var representation = new Representation(
                                dataType: Utilities.GetNexusDataTypeFromType(campbellVariable.DataType),
                                samplePeriod: samplePeriod);

                            if (!TryEnforceNamingConvention(campbellVariable.Name, out var resourceId))
                                continue;

                            var resource = new ResourceBuilder(id: resourceId)
                                .WithUnit(campbellVariable.Unit)
                                .WithGroups(fileSourceId)
                                .WithFileSourceId(fileSourceId)
                                .WithOriginalName(campbellVariable.Name)
                                .AddRepresentation(representation)
                                .Build();

                            newCatalogBuilder.AddResource(resource);
                        }
                        catch (NotSupportedException) // for invalid data type
                        {
                            // skip
                        }
                    }

                    catalog = catalog.Merge(newCatalogBuilder.Build());
                }
            }
        }

        return Task.FromResult(catalog);
    }

    protected override Task ReadAsync(
        ReadInfo info, 
        ReadRequest[] readRequests, 
        CancellationToken cancellationToken)
    {
        return Task.Run(async () =>
        {
            foreach (var readRequest in readRequests)
            {
                using var campbellFile = new CampbellFile(info.FilePath);
                var fileSourceProvider = await GetFileSourceProviderAsync(cancellationToken);

                var campbellVariable = campbellFile.Variables.First(current => current.Name == readRequest.OriginalResourceName);
                var (timeStamps, data) = campbellFile.Read<byte>(campbellVariable);
                var result = data.Buffer;
                var elementSize = readRequest.CatalogItem.Representation.ElementSize;

                cancellationToken.ThrowIfCancellationRequested();

                // write data
                if (result.Length == info.FileLength * elementSize)
                {
                    var offset = (int)info.FileOffset * elementSize;
                    var length = (int)info.FileBlock * elementSize;

                    result
                        .AsMemory()
                        .Slice(offset, length)
                        .CopyTo(readRequest.Data);

                    readRequest
                        .Status
                        .Span
                        .Fill(1);
                }
                // skip data
                else
                {
                    Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                }
            }
        }, cancellationToken);
    }

    private static bool TryEnforceNamingConvention(
        string resourceId,
        [NotNullWhen(returnValue: true)] out string newResourceId)
    {
        newResourceId = resourceId;
        newResourceId = Resource.InvalidIdCharsExpression.Replace(newResourceId, "");
        newResourceId = Resource.InvalidIdStartCharsExpression.Replace(newResourceId, "");

        return Resource.ValidIdExpression.IsMatch(newResourceId);
    }
}
