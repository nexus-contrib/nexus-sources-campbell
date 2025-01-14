# Nexus.Sources.Campbell

This data source extension makes it possible to read data files in the Campbell DAT format into Nexus.

To use it, put a `config.json` with the following sample content into the database root folder:

```json
{
  "/A/B/C": {
    "FileSourceGroups": [
      {
        "Name": "aaaa",
        "PathSegments": [
          "'DATA'"
        ],
        "FileTemplate": "yyyyMMdd_HH'_aaaa_115m_55m????.dat'",
        "FileDateTimePreselector": "(.{11})_aaaa",
        "FileDateTimeSelector": "yyyyMMdd_HH",
        "FilePeriod": "01:00:00",
        "AdditionalProperties": {
          "SamplePeriod": "00:00:00.050"
        }
      },
      {
        "Name": "bbbbb",
        "PathSegments": [
          "'DATA'"
        ],
        "FileTemplate": "yyyyMMdd_HH'_bbbbb_25m????.dat'",
        "FileDateTimePreselector": "(.{11})_bbbbb",
        "FileDateTimeSelector": "yyyyMMdd_HH",
        "FilePeriod": "01:00:00",
        "UtcOffset": "00:00:00",
        "AdditionalProperties": {
          "SamplePeriod": "00:00:00.050"
        }
      }
    ]
  }
}
```

Please see the [tests](tests/Nexus.Sources.Campbell.Tests) folder for a complete sample.