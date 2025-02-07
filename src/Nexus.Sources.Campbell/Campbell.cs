using CampbellDAT;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;
using System.Diagnostics.CodeAnalysis;

namespace Nexus.Sources;

/// <summary>
/// Additional extension-specific settings.
/// </summary>
/// <param name="TitleMap">The catalog ID to title map. Add an entry here to specify a custom catalog title.</param>
public record CampbellSettings(
    Dictionary<string, string> TitleMap
);

/// <summary>
/// Additional file source settings.
/// </summary>
/// <param name="SamplePeriod">The period between samples.</param>
/// <param name="CatalogSourceFiles">The source files to populate the catalog with resources.</param>
public record CampbellAdditionalFileSourceSettings(
    TimeSpan SamplePeriod,
    string[]? CatalogSourceFiles
);

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

[ExtensionDescription(
    "Provides access to databases with Campbell DAT files.",
    "https://github.com/Apollo3zehn/nexus-sources-campbell",
    "https://github.com/Apollo3zehn/nexus-sources-campbell")]
public class Campbell : StructuredFileDataSource<CampbellSettings, CampbellAdditionalFileSourceSettings>
{
    protected override Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(
        string path,
        CancellationToken cancellationToken
    )
    {
        if (path == "/")
        {
            return Task.FromResult(Context.SourceConfiguration.FileSourceGroupsMap
                .Select(entry =>
                    {
                        Context.SourceConfiguration.AdditionalSettings.TitleMap.TryGetValue(entry.Key, out var title);
                        return new CatalogRegistration(entry.Key, title);
                    }
                ).ToArray());
        }

        else
        {
            return Task.FromResult(Array.Empty<CatalogRegistration>());
        }
    }

    protected override Task<ResourceCatalog> EnrichCatalogAsync(
        ResourceCatalog catalog,
        CancellationToken cancellationToken
    )
    {
        var fileSourceGroupsMap = Context.SourceConfiguration.FileSourceGroupsMap[catalog.Id];

        foreach (var (fileSourceId, fileSourceGroup) in fileSourceGroupsMap)
        {
            foreach (var fileSource in fileSourceGroup)
            {
                var additionalSettings = fileSource.AdditionalSettings;
                var filePaths = default(string[]);

                if (additionalSettings.CatalogSourceFiles is not null)
                {
                    filePaths = additionalSettings.CatalogSourceFiles
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
                            var samplePeriod = fileSource.AdditionalSettings.SamplePeriod;

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
        ReadInfo<CampbellAdditionalFileSourceSettings> info, 
        ReadRequest[] readRequests, 
        CancellationToken cancellationToken
    )
    {
        return Task.Run(() =>
        {
            foreach (var readRequest in readRequests)
            {
                using var campbellFile = new CampbellFile(info.FilePath);

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
        [NotNullWhen(returnValue: true)] out string newResourceId
    )
    {
        newResourceId = resourceId;
        newResourceId = Resource.InvalidIdCharsExpression.Replace(newResourceId, "");
        newResourceId = Resource.InvalidIdStartCharsExpression.Replace(newResourceId, "");

        return Resource.ValidIdExpression.IsMatch(newResourceId);
    }
}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member