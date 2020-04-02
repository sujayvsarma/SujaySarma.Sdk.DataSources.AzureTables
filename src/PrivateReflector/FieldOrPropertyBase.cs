using SujaySarma.Sdk.DataSources.AzureTables.Attributes;
using SujaySarma.Sdk.DataSources.AzureTables.EdmConverters;

using System;
using System.Reflection;

namespace SujaySarma.Sdk.DataSources.AzureTables.PrivateReflector
{
    /// <summary>
    /// Common properties and methods for fields and properties
    /// </summary>
    internal class FieldOrPropertyBase
    {
        /// <summary>
        /// Name of the property or field
        /// </summary>
        public string MemberName { get; private set; }

        /// <summary>
        /// Flag indicating if the member can be written (has a Setter)
        /// </summary>
        public bool CanWrite { get; private set; } = true;

        /// <summary>
        /// Data Type of the member
        /// </summary>
        public Type Type { get; private set; } = typeof(object);

        /// <summary>
        /// The typecode of the actual data type for this property/field
        /// </summary>
        public TypeCode TypeCode { get; private set; } = TypeCode.Object;

        /// <summary>
        /// If true, the Type is supported by Edm
        /// </summary>
        public bool IsEdmType { get; private set; } = false;

        /// <summary>
        /// If true, the CLR type allows NULLs
        /// </summary>
        public bool IsNullableType { get; private set; } = true;

        /// <summary>
        /// Maps to the TableEntity's PartitionKey
        /// </summary>
        public bool IsPartitionKey { get; private set; } = false;

        /// <summary>
        /// Maps to the TableEntity's RowKey
        /// </summary>
        public bool IsRowKey { get; set; } = false;

        /// <summary>
        /// Maps to the TableEntity's ETag property
        /// </summary>
        public bool IsETag { get; set; } = false;

        /// <summary>
        /// Maps to the TableEntity's Timestamp property
        /// </summary>
        public bool IsTimestamp { get; set; } = false;


        /// <summary>
        /// If mapped to a TableEntity's fields, reference to the TableColumnAttribute for that.
        /// (Could be NULL)
        /// </summary>
        public TableColumnAttribute? TableEntityColumn { get; set; }

        /// <summary>
        /// Initialize the structure
        /// </summary>
        /// <param name="property">The property</param>
        protected FieldOrPropertyBase(System.Reflection.PropertyInfo property)
        {
            MemberName = property.Name;
            CanWrite = property.CanWrite;
            CommonInit(property, property.PropertyType);
        }

        /// <summary>
        /// Initialize the structure
        /// </summary>
        /// <param name="field">The field</param>
        protected FieldOrPropertyBase(System.Reflection.FieldInfo field)
        {
            MemberName = field.Name;
            CanWrite = (!field.IsInitOnly);
            CommonInit(field, field.FieldType);
        }

        /// <summary>
        /// Common initialization
        /// </summary>
        /// <param name="member">MemberInfo/param>
        /// <param name="dataType">Type of property/field</param>
        private void CommonInit(System.Reflection.MemberInfo member, Type dataType)
        {
            Type = dataType;

            foreach (Attribute attribute in member.GetCustomAttributes(true))
            {
                if (attribute is PartitionKeyAttribute)
                {
                    IsPartitionKey = true;
                }
                else if (attribute is RowKeyAttribute)
                {
                    IsRowKey = true;
                }
                else if (attribute is ETagAttribute)
                {
                    IsETag = true;
                }
                else if (attribute is TimestampAttribute)
                {
                    IsTimestamp = true;
                }
                else if (attribute is TableColumnAttribute tc)
                {
                    // Now the problem is legacy code that may define columns we now have attributes for 
                    // as TableColumn() with matching names. So filter those out.

                    switch (tc.ColumnName)
                    {
                        case "ETag":
                            IsETag = true;
                            continue;

                        case "Timestamp":
                            IsTimestamp = true;
                            continue;

                        case "PartitionKey":
                            IsPartitionKey = true;
                            continue;

                        case "RowKey":
                            IsRowKey = true;
                            continue;
                    }

                    // Traditional column
                    TableEntityColumn = tc;
                }
            }

            Type? underlyingType = Nullable.GetUnderlyingType(Type);
            TypeCode = Type.GetTypeCode(underlyingType ?? Type);
            IsEdmType = EdmTypeConverter.IsEdmCompatibleType(underlyingType ?? Type);

            IsNullableType = (dataType == typeof(string)) || (underlyingType == typeof(string)) || (underlyingType != null);
        }

        /// <summary>
        /// Read the value from the property/field and return it
        /// </summary>
        /// <typeparam name="ObjType">Data type of the object to read the property/field of</typeparam>
        /// <param name="obj">The object instance to read the value from</param>
        /// <returns>The value</returns>
        public virtual object? Read<ObjType>(ObjType obj) => null;

        /// <summary>
        /// Writes the provided value to the property/field of the object instance
        /// </summary>
        /// <typeparam name="ObjType">Data type of the object to read the property/field of</typeparam>
        /// <param name="obj">The object instance to write the value to</param>
        /// <param name="value">Value to write out</param>
        public virtual void Write<ObjType>(ObjType obj, object? value) { }


        /// <summary>
        /// Binding flags for read & write of properties/fields
        /// </summary>
        protected readonly BindingFlags FLAGS_READ_WRITE = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static;
    }
}
