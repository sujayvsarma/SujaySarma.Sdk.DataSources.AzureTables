using System;
using System.ComponentModel;

namespace SujaySarma.Sdk.DataSources.AzureTables.EdmConverters
{
    /// <summary>
    /// Provides conversion from Edm types to .NET types
    /// </summary>
    internal static class EdmTypeConverter
    {
        /// <summary>
        /// Returns if the value needs to be converted between the two types
        /// </summary>
        /// <param name="edmType">The data type in Azure Table</param>
        /// <param name="clrType">The .NET type</param>
        /// <returns>True if type needs to be converted</returns>
        public static bool NeedsConversion(Type edmType, Type clrType)
        {
            if (!IsEdmCompatibleType(clrType))
            {
                throw new TypeLoadException($"'{clrType.Name}' is not compatible for Edm.");
            }

            return (Type.GetTypeCode(clrType) != Type.GetTypeCode(edmType));
        }

        /// <summary>
        /// Convert between types
        /// </summary>
        /// <param name="destinationType">CLR Type of destination</param>
        /// <param name="value">The value to convert</param>
        /// <returns>The converted value</returns>
        public static object ConvertTo(Type destinationType, object value)
        {
            TypeConverter converter = TypeDescriptor.GetConverter(destinationType);
            if ((converter == null) || (!converter.CanConvertTo(destinationType)))
            {
                throw new TypeLoadException($"Could not find type converters for '{destinationType.Name}' type.");
            }

            return converter.ConvertTo(value, destinationType);
        }


        public static bool IsEdmCompatibleType(Type clrType)
            => (
                       (clrType.IsEnum)
                    || (clrType == typeof(string))
                    || (clrType == typeof(byte[]))
                    || (clrType == typeof(bool))
                    || (clrType == typeof(DateTime))
                    || (clrType == typeof(DateTimeOffset))
                    || (clrType == typeof(double))
                    || (clrType == typeof(Guid))
                    || (clrType == typeof(int)) || (clrType == typeof(uint))
                    || (clrType == typeof(long)) || (clrType == typeof(ulong))
                    || (clrType == typeof(byte?[]))
                    || (clrType == typeof(bool?))
                    || (clrType == typeof(DateTime?))
                    || (clrType == typeof(DateTimeOffset?))
                    || (clrType == typeof(double?))
                    || (clrType == typeof(Guid?))
                    || (clrType == typeof(int?)) || (clrType == typeof(uint?))
                    || (clrType == typeof(long?)) || (clrType == typeof(ulong?))
                );

    }
}
