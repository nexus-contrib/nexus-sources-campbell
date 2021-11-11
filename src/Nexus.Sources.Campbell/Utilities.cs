using Nexus.DataModel;
using System;

namespace Nexus.Sources.Campbell
{
    public class Utilities
    {
        public static NexusDataType GetNexusDataTypeFromType(Type type)
        {
            return true switch
            {
                true when type == typeof(Byte) => NexusDataType.UINT8,
                true when type == typeof(SByte) => NexusDataType.INT8,
                true when type == typeof(UInt16) => NexusDataType.UINT16,
                true when type == typeof(Int16) => NexusDataType.INT16,
                true when type == typeof(UInt32) => NexusDataType.UINT32,
                true when type == typeof(Int32) => NexusDataType.INT32,
                true when type == typeof(UInt64) => NexusDataType.UINT64,
                true when type == typeof(Int64) => NexusDataType.INT64,
                true when type == typeof(Single) => NexusDataType.FLOAT32,
                true when type == typeof(Double) => NexusDataType.FLOAT64,
                _ => throw new NotSupportedException()
            };
        }
    }
}
