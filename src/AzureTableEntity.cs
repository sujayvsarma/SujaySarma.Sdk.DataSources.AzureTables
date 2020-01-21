using Microsoft.Azure.Cosmos.Table;

using SujaySarma.Sdk.DataSources.AzureTables.Attributes;
using SujaySarma.Sdk.DataSources.AzureTables.EdmConverters;
using SujaySarma.Sdk.DataSources.AzureTables.PrivateReflector;
using SujaySarma.Sdk.DataSources.AzureTables.Utility;

using System;
using System.Collections.Generic;

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
                if (!TableKeyValidator.IsValid(value))
                {
                    throw new ArgumentException(nameof(PartitionKey));
                }

                _partitionKey = value;
            }
        }
        private string _partitionKey = string.Empty;

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
        private string _rowKey = string.Empty;

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

        private IDictionary<string, object?> _properties = new Dictionary<string, object?>();
        internal static string PROPERTY_NAME_ISDELETED = "IsDeleted";

        /// <summary>
        /// Adds or updates a property. If property already exists, updates the value. Otherwise adds a new property.
        /// </summary>
        /// <param name="name">Name of the property</param>
        /// <param name="value">Value of the property</param>
        public void AddOrUpdateProperty(string name, object? value)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException("name");
            }

            if (_properties.ContainsKey(name))
            {
                _properties[name] = value;
            }
            else
            {
                _properties.Add(name, value);
            }
        }

        /// <summary>
        /// Returns the value of the specified property
        /// </summary>
        /// <param name="name">Name of the property</param>
        /// <param name="defaultValue">Value to return if the property does not exist</param>
        /// <returns>Value of specified property</returns>
        public object? GetPropertyValue(string name, object? defaultValue = null)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException("name");
            }

            if (!_properties.ContainsKey(name))
            {
                return defaultValue;
            }

            if (_properties[name] == null)
            {
                return defaultValue;
            }

            object? value = _properties[name];
            if (value == null)
            {
                return null;
            }

            if (value is EntityProperty)
            {
                return ((EntityProperty)value).PropertyAsObject;
            }

            return value;
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
        /// <param name="forDelete">If TRUE, only the partition and row key data is extracted. We don't waste cycles populating other properties</param>
        /// <typeparam name="T">Type of the business object instance</typeparam>
        /// <returns>The instantiated TableEntity</returns>
        public static AzureTableEntity From<T>(T instance, bool forDelete = false)
            where T : class
        {
            if (instance == null)
            {
                throw new ArgumentNullException(nameof(instance));
            }

            AzureTableEntity entity = new AzureTableEntity();

            ClassInformation? objectInfo = TypeInspector.InspectForAzureTables<T>();
            if (objectInfo == null)
            {
                throw new TypeLoadException($"Type '{typeof(T).FullName}' is not anotated with the '{typeof(TableAttribute).FullName}' attribute or has no properties/fields mapped to an Azure table.");
            }

            bool hasPartitionKey = false, hasRowKey = false;
            foreach (FieldOrPropertyBase member in objectInfo.FieldsOrProperties)
            {
                object? value = null;

                if (member.IsPartitionKey)
                {
                    if (hasPartitionKey)
                    {
                        throw new TypeLoadException($"Object '{objectInfo.FullyQualifiedName}' contains multiple partition key definitions.");
                    }

                    value = member.Read(instance);
                    if (value == null)
                    {
                        throw new Exception("PartitionKey must be NON NULL.");
                    }

                    if (!(GetAcceptableValue(member.Type, typeof(string), value) is string pk1))
                    {
                        throw new InvalidOperationException("PartitionKey cannot be NULL.");
                    }
                    entity.PartitionKey = pk1;
                    hasPartitionKey = true;
                }
                if (member.IsRowKey)
                {
                    if (hasRowKey)
                    {
                        throw new TypeLoadException($"Object '{objectInfo.FullyQualifiedName}' contains multiple row key definitions.");
                    }

                    value = member.Read(instance);
                    if (value == null)
                    {
                        throw new Exception("RowKey must be NON NULL.");
                    }

                    if (!(GetAcceptableValue(member.Type, typeof(string), value) is string rk1))
                    {
                        throw new InvalidOperationException("PartitionKey cannot be NULL.");
                    }
                    entity.RowKey = rk1;
                    hasRowKey = true;
                }
                if ((!forDelete) && (member.TableEntityColumn != null))
                {
                    entity.AddOrUpdateProperty(
                            member.TableEntityColumn.ColumnName,
                            GetAcceptableValue(member.Type, (member.IsEdmType ? member.Type : typeof(string)), member.Read(instance))
                        );
                }

                if (forDelete && hasPartitionKey && hasRowKey)
                {
                    break;
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
            _properties = new Dictionary<string, object?>();

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
            ClassInformation? objectInfo = TypeInspector.InspectForAzureTables<T>();
            if (objectInfo == null)
            {
                throw new TypeLoadException($"Type '{typeof(T).FullName}' is not anotated with the '{typeof(TableAttribute).FullName}' attribute.");
            }

            T instance = new T();
            foreach (FieldOrPropertyBase member in objectInfo.FieldsOrProperties)
            {
                if (member.IsPartitionKey)
                {
                    member.Write(instance, GetAcceptableValue(typeof(string), member.Type, PartitionKey));
                }

                if (member.IsRowKey)
                {
                    member.Write(instance, GetAcceptableValue(typeof(string), member.Type, RowKey));
                }

                if (member.TableEntityColumn != null)
                {
                    switch (member.TableEntityColumn.ColumnName)
                    {
                        case "ETag":
                            member.Write(instance, GetAcceptableValue(typeof(string), member.Type, ETag));
                            break;

                        case "Timestamp":
                            member.Write(instance, GetAcceptableValue(typeof(DateTimeOffset), member.Type, Timestamp));
                            break;

                        case "PartitionKey":
                            member.Write(instance, GetAcceptableValue(typeof(string), member.Type, PartitionKey));
                            break;

                        case "RowKey":
                            member.Write(instance, GetAcceptableValue(typeof(string), member.Type, RowKey));
                            break;

                        default:
                            object? value = GetPropertyValue(member.TableEntityColumn.ColumnName, default);
                            member.Write(instance,
                                    ((value == default) || (value == null))
                                    ? default
                                    : GetAcceptableValue(value.GetType(), member.Type, value)
                                );
                            break;
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
            ClassInformation? objectInfo = TypeInspector.InspectForAzureTables<T>();
            if (objectInfo == null)
            {
                throw new TypeLoadException($"Type '{typeof(T).FullName}' is not anotated with the '{typeof(TableAttribute).FullName}' attribute.");
            }

            List<T> list = new List<T>();
            if ((records != null) && (records.Count > 0))
            {
                foreach (AzureTableEntity tableEntity in records)
                {
                    T instance = new T();

                    foreach (FieldOrPropertyBase member in objectInfo.FieldsOrProperties)
                    {
                        if (member.IsPartitionKey)
                        {
                            member.Write(instance, GetAcceptableValue(typeof(string), member.Type, tableEntity.PartitionKey));
                        }

                        if (member.IsRowKey)
                        {
                            member.Write(instance, GetAcceptableValue(typeof(string), member.Type, tableEntity.RowKey));
                        }

                        if (member.TableEntityColumn != null)
                        {
                            switch (member.TableEntityColumn.ColumnName)
                            {
                                case "ETag":
                                    member.Write(instance, GetAcceptableValue(typeof(string), member.Type, tableEntity.ETag));
                                    break;

                                case "Timestamp":
                                    member.Write(instance, GetAcceptableValue(typeof(DateTimeOffset), member.Type, tableEntity.Timestamp));
                                    break;

                                case "PartitionKey":
                                    member.Write(instance, GetAcceptableValue(typeof(string), member.Type, tableEntity.PartitionKey));
                                    break;

                                case "RowKey":
                                    member.Write(instance, GetAcceptableValue(typeof(string), member.Type, tableEntity.RowKey));
                                    break;

                                default:
                                    object? value = tableEntity.GetPropertyValue(member.TableEntityColumn.ColumnName, default);
                                    member.Write(instance,
                                            ((value == default) || (value == null))
                                            ? null
                                            : GetAcceptableValue(value.GetType(), member.Type, value)
                                        );
                                    break;
                            }
                        }
                    }

                    list.Add(instance);
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

        /// <summary>
        /// Returns a value that matches the destination type
        /// </summary>
        /// <param name="sourceType">Type of value being provided</param>
        /// <param name="destinationType">Type of the destination container</param>
        /// <param name="value">Value to convert/change</param>
        /// <returns>The value of type destinationType</returns>
        private static object? GetAcceptableValue(Type sourceType, Type destinationType, object? value)
        {
            Type? srcActualType = Nullable.GetUnderlyingType(sourceType);
            Type convertFromType = srcActualType ?? sourceType;

            Type? destActualType = Nullable.GetUnderlyingType(destinationType);
            Type convertToType = destActualType ?? destinationType;

            if (value == null)
            {
                return null;
            }

            if (EdmTypeConverter.NeedsConversion(convertFromType, convertToType))
            {
                return EdmTypeConverter.ConvertTo(convertToType, value);
            }

            return value;
        }

        #endregion
    }
}
