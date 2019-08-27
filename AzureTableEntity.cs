using Microsoft.Azure.Cosmos.Table;

using SujaySarma.Sdk.DataSources.AzureTables.Attributes;
using SujaySarma.Sdk.DataSources.AzureTables.Utility;
using SujaySarma.Sdk.Core.Reflection;

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text.Json;

namespace SujaySarma.Sdk.DataSources.AzureTables
{
    /// <summary>
    /// A TableEntity implementation that avoids us having to write TableEntity classes for every 
    /// business object we have in the system!
    /// </summary>
    public class AzureTableEntity : ITableEntity
    {

        #region Properties

        /// <summary>
        /// The partition key
        /// </summary>
        public string PartitionKey
        {
            get => _partitionKey;
            set
            {
                if (! TableKeyValidator.IsValid(value))
                {
                    throw new ArgumentException(nameof(PartitionKey));
                }

                _partitionKey = value;
            }
        }
        private string _partitionKey = null;

        /// <summary>
        /// The row key
        /// </summary>
        public string RowKey
        {
            get => _rowKey;
            set
            {
                if (!TableKeyValidator.IsValid(value))
                {
                    throw new ArgumentException(nameof(RowKey));
                }

                _rowKey = value;
            }
        }
        private string _rowKey = null;

        /// <summary>
        /// The LastModified timestamp of the row
        /// </summary>
        public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.MinValue;

        /// <summary>
        /// The E-Tag value of the row. Set to "*" to perform a blind update
        /// </summary>
        public string ETag { get; set; } = "*";

        #endregion

        #region Properties -- Expando Implementation

        private IDictionary<string, object> _properties = new Dictionary<string, object>();
        internal static string PROPERTY_NAME_ISDELETED = "IsDeleted";

        /// <summary>
        /// Adds or updates a property. If property already exists, updates the value. Otherwise adds a new property.
        /// </summary>
        /// <param name="name">Name of the property</param>
        /// <param name="value">Value of the property</param>
        public void AddOrUpdateProperty(string name, object value)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException("name");
            }

            if (!_properties.ContainsKey(name))
            {
                _properties.Add(name, null);
            }

            object storedValue = value;
            if ((storedValue != null) && (!IsEdmCompatibleType(storedValue.GetType())))
            {
                // serialize it to Json and store that
                storedValue = JsonSerializer.Serialize(value);
            }

            _properties[name] = storedValue;

        }

        /// <summary>
        /// Returns the value of the specified property
        /// </summary>
        /// <param name="name">Name of the property</param>
        /// <param name="defaultValue">Value to return if the property does not exist</param>
        /// <returns>Value of specified property</returns>
        public object GetPropertyValue(string name, object defaultValue = null)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException("name");
            }

            if (!_properties.ContainsKey(name))
            {
                return defaultValue;
            }

            return ((EntityProperty)_properties[name]).PropertyAsObject;
        }

        // tests if the value can be stored directly into the Azure table
        private static bool IsEdmCompatibleType(Type clrType)
        {
            bool isEdmType = (
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
                );

            isEdmType = isEdmType || (
                       (clrType == typeof(byte?[]))
                    || (clrType == typeof(bool?))
                    || (clrType == typeof(DateTime?))
                    || (clrType == typeof(DateTimeOffset?))
                    || (clrType == typeof(double?))
                    || (clrType == typeof(Guid?))
                    || (clrType == typeof(int?)) || (clrType == typeof(uint?))
                    || (clrType == typeof(long?)) || (clrType == typeof(ulong?))
                );

            return isEdmType;
        }

        #endregion

        #region Constructors

        /// <summary>
        /// Blank constructor, used by the rehydrator
        /// </summary>
        public AzureTableEntity()
        {
            Timestamp = DateTimeOffset.UtcNow;
            AddOrUpdateProperty(PROPERTY_NAME_ISDELETED, false);

            // without this data cannot be updated
            ETag = "*";
        }

        /// <summary>
        /// Create a table entity from a business object
        /// </summary>
        /// <param name="instance">Business object instance</param>
        /// <typeparam name="T">Type of the business object instance</typeparam>
        /// <returns>The instantiated TableEntity</returns>
        public static AzureTableEntity From<T>(T instance)
            where T : class
        {
            if (instance == null)
            {
                throw new ArgumentNullException(nameof(instance));
            }

            AzureTableEntity entity = new AzureTableEntity();

            ObjectMetadataInfo objectInfo = TypeInspector.GetObjectMetadataInfo<T>();

            bool hasPartitionKey = false, hasRowKey = false;
            foreach (PropertyFieldMetadataInfo propertyInfo in objectInfo.PropertiesAndFields)
            {
                bool isPartitionKey = false, isRowKey = false;
                string entityPropertyName = null;
                object value = null;

                foreach (Attribute attribute in propertyInfo.CustomAttributes)
                {
                    switch (attribute)
                    {
                        case PartitionKeyAttribute pk:
                            {
                                if (hasPartitionKey)
                                {
                                    throw new TypeLoadException($"Object '{objectInfo.FullyQualifiedName}' contains multiple partition key definitions.");
                                }

                                isPartitionKey = true;
                                value = propertyInfo.Read(instance);
                                break;
                            }

                        case RowKeyAttribute rk:
                            {
                                if (hasRowKey)
                                {
                                    throw new TypeLoadException($"Object '{objectInfo.FullyQualifiedName}' contains multiple row key definitions.");
                                }

                                isRowKey = true;
                                value = propertyInfo.Read(instance);
                                break;
                            }

                        case TableColumnAttribute tc:
                            {
                                if (!IsEdmCompatibleType(propertyInfo.Type))
                                {
                                    try
                                    {
                                        // try serializing to Json?
                                        value = Newtonsoft.Json.JsonConvert.SerializeObject(propertyInfo.Read(instance));
                                    }
                                    catch
                                    {
                                        throw new TypeLoadException($"The type '{propertyInfo.Type.FullName}' for '{objectInfo.ClassName}.{propertyInfo.MemberName}' is not an Edm type usable with Azure Tables.");
                                    }
                                }
                                else
                                {
                                    value = propertyInfo.Read(instance);
                                }

                                entityPropertyName = tc.ColumnName;
                                break;
                            }
                    }
                }

                if (isPartitionKey)
                {
                    if (value == null)
                    {
                        throw new Exception("PartitionKey must be NON NULL.");
                    }

                    string pkValue = value.ToString();
                    if (propertyInfo.Type != typeof(string))
                    {
                        TypeConverter pkc = TypeDescriptor.GetConverter(propertyInfo.Type);
                        if ((pkc == null) || (!pkc.CanConvertTo(typeof(string))))
                        {
                            throw new TypeLoadException($"The type '{objectInfo.FullyQualifiedName}.{propertyInfo.MemberName}' cannot be converted to a string equivalent.");
                        }
                        pkValue = pkc.ConvertToString(value);
                    }

                    entity.PartitionKey = pkValue;
                    hasPartitionKey = true;
                }

                if (isRowKey)
                {
                    if (value == null)
                    {
                        throw new Exception("RowKey must be NON NULL.");
                    }

                    string rkValue = value.ToString();
                    if (propertyInfo.Type != typeof(string))
                    {
                        TypeConverter pkc = TypeDescriptor.GetConverter(propertyInfo.Type);
                        if ((pkc == null) || (!pkc.CanConvertTo(typeof(string))))
                        {
                            throw new TypeLoadException($"The type '{objectInfo.FullyQualifiedName}.{propertyInfo.MemberName}' cannot be converted to a string equivalent.");
                        }

                        rkValue = pkc.ConvertToString(value);
                    }

                    entity.RowKey = rkValue;
                    hasRowKey = true;
                }

                if (entityPropertyName != null)
                {
                    entity.AddOrUpdateProperty(entityPropertyName, value);
                }
            }

            return entity;
        }

        #endregion

        #region ITableEntity Methods

        /// <summary>
        /// Rehydrate the table entity from storage
        /// </summary>
        /// <param name="properties">The extra properties read from the Azure table storage</param>
        /// <param name="_">OperationContext, not used by this method</param>
        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext _)
        {
            _properties = new Dictionary<string, object>();

            foreach (string propertyName in properties.Keys)
            {
                _properties.Add(propertyName, properties[propertyName]);
            }
        }

        /// <summary>
        /// Persist the table entity to storage
        /// </summary>
        /// <param name="_">OperationContext, not used by this method</param>
        /// <returns>Dictionary of properties to persist</returns>
        public IDictionary<string, EntityProperty> WriteEntity(OperationContext _)
        {
            Dictionary<string, EntityProperty> entityProperties = new Dictionary<string, EntityProperty>();
            foreach (string propertyName in _properties.Keys)
            {
                entityProperties.Add(propertyName, EntityProperty.CreateEntityPropertyFromObject(_properties[propertyName]));
            }

            return entityProperties;
        }

        #endregion

        #region Methods

        /// <summary>
        /// Convert the current Table Entity object into the provided business object
        /// </summary>
        /// <typeparam name="T">Type of the business object instance</typeparam>
        /// <returns>Business object instance -- NULL if current entity is not properly populated.</returns>
        public T To<T>()
            where T : class, new()
        {
            string localPK = PartitionKey, localRK = RowKey;
            if (string.IsNullOrWhiteSpace(localPK) || string.IsNullOrWhiteSpace(localRK) || ((bool)GetPropertyValue(PROPERTY_NAME_ISDELETED, false)))
            {
                // no representation
                return null;
            }

            T instance = new T();
            ObjectMetadataInfo objectInfo = TypeInspector.GetObjectMetadataInfo<T>();
            foreach (PropertyFieldMetadataInfo propertyInfo in objectInfo.PropertiesAndFields)
            {
                foreach (Attribute attribute in propertyInfo.CustomAttributes)
                {
                    switch (attribute)
                    {
                        case PartitionKeyAttribute pk:
                            {
                                propertyInfo.Write(ref instance, PartitionKey);
                                break;
                            }

                        case RowKeyAttribute rk:
                            {
                                propertyInfo.Write(ref instance, RowKey);
                                break;
                            }

                        case TableColumnAttribute tc:
                            {
                                object value = default;

                                switch (tc.ColumnName)
                                {
                                    case "ETag":
                                        value = ETag;
                                        break;

                                    case "Timestamp":
                                        value = Timestamp.UtcDateTime;
                                        break;

                                    case "PartitionKey":
                                        value = PartitionKey;
                                        break;

                                    case "RowKey":
                                        value = RowKey;
                                        break;

                                    default:
                                        value = GetPropertyValue(tc.ColumnName, default);
                                        if (propertyInfo.Type == typeof(DateTime))
                                        {
                                            if (value == null)
                                            {
                                                value = DateTime.MinValue;
                                            }

                                            // Azure stores dt in UTC, change it back
                                            value = ((DateTime)value).ToLocalTime();
                                        }
                                        else if (propertyInfo.Type == typeof(DateTime?))
                                        {
                                            if (value != null)
                                            {
                                                // Azure stores dt in UTC, change it back
                                                value = ((DateTime?)value).Value.ToLocalTime();
                                            }
                                        }
                                        break;
                                }

                                try
                                {
                                    propertyInfo.Write(ref instance, value);
                                }
                                catch
                                {
                                    // try deserializing
                                    propertyInfo.Write(ref instance, JsonSerializer.Deserialize(value as string, propertyInfo.Type));
                                }
                                break;
                            }
                    }
                }
            }

            return instance;
        }

        /// <summary>
        /// Converts the provided list of table entity records to a list of business objects
        /// </summary>
        /// <typeparam name="T">Type of the business object instance</typeparam>
        /// <param name="records">AzureTableEntity records to process</param>
        /// <returns>List of business objects. An empty list if records were empty</returns>
        public static List<T> ToList<T>(List<AzureTableEntity> records)
            where T : class, new()
        {
            List<T> list = new List<T>();

            if ((records != null) && (records.Count > 0))
            {
                foreach (AzureTableEntity tableEntity in records)
                {
                    list.Add(tableEntity.To<T>());
                }
            }

            return list;
        }

        /// <summary>
        /// Overwrites existing properties with values from the <paramref name="entity"/>. Properties that do not 
        /// exist in <paramref name="entity"/> are not touched.
        /// </summary>
        /// <param name="entity">The entity to copy values from</param>
        public void ImportValues(AzureTableEntity entity)
        {
            PartitionKey = entity.PartitionKey;
            RowKey = entity.RowKey;

            List<string> propertyNames = new List<string>();
            foreach (string key in _properties.Keys)
            {
                propertyNames.Add(key);
            }

            foreach (string propertyName in propertyNames)
            {
                if (entity._properties.ContainsKey(propertyName))
                {
                    _properties[propertyName] = entity._properties[propertyName];
                }
            }
        }


        #endregion
    }
}
