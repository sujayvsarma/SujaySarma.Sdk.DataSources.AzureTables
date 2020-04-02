using System.Collections.Generic;

namespace SujaySarma.Sdk.DataSources.AzureTables.PrivateReflector
{
    /// <summary>
    /// Structure to cache metadata for objects, their constructors and properties
    /// </summary>
    internal class ClassInformation
    {
        /// <summary>
        /// Name of the class
        /// </summary>
        public string ClassName { get; private set; }

        /// <summary>
        /// The fully qualified name of this object. Used as the object's key in the cache
        /// </summary>
        public string FullyQualifiedName { get; private set; }

        /// <summary>
        /// The Table attribute
        /// </summary>
        public AzureTables.Attributes.TableAttribute TableAttribute { get; private set; }

        /// <summary>
        /// Get/set if the class has a PartitionKey defined
        /// </summary>
        public bool HasPartitionKey { get; set; } = false;

        /// <summary>
        /// Get/set if the class has a RowKey defined
        /// </summary>
        public bool HasRowKey { get; set; } = false;

        /// <summary>
        /// Get/set if the class has a ETag property defined
        /// </summary>
        public bool HasETag { get; set; } = false;

        /// <summary>
        /// Get/set if the class has a Timestamp defined
        /// </summary>
        public bool HasTimestamp { get; set; } = false;

        /// <summary>
        /// A readonly list of the properties
        /// </summary>
        public IReadOnlyList<Property> Properties { get; private set; }

        /// <summary>
        /// A readonly list of the fields
        /// </summary>
        public IReadOnlyList<Field> Fields { get; private set; }

        /// <summary>
        /// A readonly list of both properties and fields
        /// </summary>
        public IReadOnlyList<FieldOrPropertyBase> FieldsOrProperties { get; private set; }

        /// <summary>
        /// Initialize the structure for an object
        /// </summary>
        /// <param name="localName">Name of the class</param>
        /// <param name="fullName">The fully qualified name of this object. Used as the object's key in the cache</param>
        /// <param name="table">The TableAttribute reference</param>
        /// <param name="properties">A readonly list of the properties in this object</param>
        /// <param name="fields">A readonly list of fields in this object</param>
        public ClassInformation(string localName, string fullName, AzureTables.Attributes.TableAttribute table,
                                    IReadOnlyList<Property> properties, IReadOnlyList<Field> fields)
        {
            ClassName = localName;
            FullyQualifiedName = fullName;
            TableAttribute = table;
            Properties = properties;
            Fields = fields;

            List<FieldOrPropertyBase> list = new List<FieldOrPropertyBase>();
            list.AddRange(properties);
            list.AddRange(fields);

            FieldsOrProperties = list.AsReadOnly();

        }
    }
}
