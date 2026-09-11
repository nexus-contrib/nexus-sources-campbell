using Nexus.DataModel;

namespace Nexus.Sources;

internal class Utilities
{
    public static NexusDataType GetNexusDataTypeFromType(Type type)
    {
        return true switch
        {
            true when type == typeof(Byte) => NexusDataType.UInt8,
            true when type == typeof(SByte) => NexusDataType.Int8,
            true when type == typeof(UInt16) => NexusDataType.UInt16,
            true when type == typeof(Int16) => NexusDataType.Int16,
            true when type == typeof(UInt32) => NexusDataType.UInt32,
            true when type == typeof(Int32) => NexusDataType.Int32,
            true when type == typeof(UInt64) => NexusDataType.UInt64,
            true when type == typeof(Int64) => NexusDataType.Int64,
            true when type == typeof(Single) => NexusDataType.Float32,
            true when type == typeof(Double) => NexusDataType.Float64,
            _ => throw new NotSupportedException()
        };
    }
}
