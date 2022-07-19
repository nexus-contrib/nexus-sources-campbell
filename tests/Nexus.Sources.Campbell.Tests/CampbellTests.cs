using Microsoft.Extensions.Logging.Abstractions;
using Nexus.DataModel;
using Nexus.Extensibility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Nexus.Sources.Tests
{
    public class CampbellTests
    {
        [Fact]
        public async Task ProvidesCatalog()
        {
            // arrange
            var dataSource = new Campbell() as IDataSource;

            var context = new DataSourceContext(
                ResourceLocator: new Uri("Database", UriKind.Relative),
                SystemConfiguration: default!,
                SourceConfiguration: default!,
                RequestConfiguration: default!);

            await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

            // act
            var actual = await dataSource.GetCatalogAsync("/A/B/C", CancellationToken.None);
            var actualIds = actual.Resources.Skip(9).Take(2).Select(resource => resource.Id).ToList();
            var actualUnits = actual.Resources.Skip(9).Take(2).Select(resource => resource.Properties.GetStringValue("unit")).ToList();
            var actualGroups = actual.Resources.Skip(9).Take(2).SelectMany(resource => resource.Properties.GetStringArray("groups")).ToList();
            var actualTimeRange = await dataSource.GetTimeRangeAsync("/A/B/C", CancellationToken.None);

            // assert
            var expectedIds = new List<string>() { "aaaa_55_SonicTempC", "bbbbb_25_Vx" };
            var expectedUnits = new List<string>() { "", "" };
            var expectedGroups = new List<string>() { "aaaa", "bbbbb" };
            var expectedStartDate = new DateTime(2015, 10, 05, 12, 00, 00, DateTimeKind.Utc);
            var expectedEndDate = new DateTime(2015, 10, 05, 14, 00, 00, DateTimeKind.Utc);

            Assert.True(expectedIds.SequenceEqual(actualIds));
            Assert.True(expectedUnits.SequenceEqual(actualUnits));
            Assert.True(expectedGroups.SequenceEqual(actualGroups));
            Assert.Equal(expectedStartDate, actualTimeRange.Begin);
            Assert.Equal(expectedEndDate, actualTimeRange.End);
        }

        [Fact]
        public async Task ProvidesDataAvailability()
        {
            // arrange
            var dataSource = new Campbell() as IDataSource;

            var context = new DataSourceContext(
                ResourceLocator: new Uri("Database", UriKind.Relative),
                SystemConfiguration: default!,
                SourceConfiguration: default!,
                RequestConfiguration: default!);

            await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

            // act
            var actual = new Dictionary<DateTime, double>();
            var begin = new DateTime(2015, 10, 04, 0, 0, 0, DateTimeKind.Utc);
            var end = new DateTime(2015, 10, 06, 0, 0, 0, DateTimeKind.Utc);

            var currentBegin = begin;

            while (currentBegin < end)
            {
                actual[currentBegin] = await dataSource.GetAvailabilityAsync("/A/B/C", currentBegin, currentBegin.AddDays(1), CancellationToken.None);
                currentBegin += TimeSpan.FromDays(1);
            }

            // assert
            var expected = new SortedDictionary<DateTime, double>(Enumerable.Range(0, 2).ToDictionary(
                    i => begin.AddDays(i),
                    i => 0.0));

            expected[begin.AddDays(0)] = 0;
            expected[begin.AddDays(1)] = 4 / 48.0;

            Assert.True(expected.SequenceEqual(new SortedDictionary<DateTime, double>(actual)));
        }

        [Fact]
        public async Task CanReadFullDay()
        {
            // arrange
            var dataSource = new Campbell() as IDataSource;

            var context = new DataSourceContext(
                ResourceLocator: new Uri("Database", UriKind.Relative),
                SystemConfiguration: default!,
                SourceConfiguration: default!,
                RequestConfiguration: default!);

            await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

            // act
            var catalog = await dataSource.GetCatalogAsync("/A/B/C", CancellationToken.None);
            var resource = catalog.Resources.First();
            var representation = resource.Representations.First();
            var catalogItem = new CatalogItem(catalog, resource, representation);

            var begin = new DateTime(2015, 10, 05, 0, 0, 0, DateTimeKind.Utc);
            var end = new DateTime(2015, 10, 06, 0, 0, 0, DateTimeKind.Utc);
            var (data, status) = ExtensibilityUtilities.CreateBuffers(representation, begin, end);

            var result = new ReadRequest(catalogItem, data, status);
            await dataSource.ReadAsync(begin, end, new ReadRequest[] { result }, default!, new Progress<double>(), CancellationToken.None);

            // assert
            void DoAssert()
            {
                var data = MemoryMarshal.Cast<byte, float>(result.Data.Span);

                Assert.Equal(0, data[864000 - 1]);
                Assert.Equal(8.590, data[864000 + 0], precision: 3);
                Assert.Equal(6.506, data[1008000 - 1], precision: 3);
                Assert.Equal(0, data[1008000 + 0]);

                Assert.Equal(0, result.Status.Span[864000 - 1]);
                Assert.Equal(1, result.Status.Span[864000 + 0]);
                Assert.Equal(1, result.Status.Span[1008000 - 1]);
                Assert.Equal(0, result.Status.Span[1008000 + 0]);
            }

            DoAssert();
        }
    }
}