using SujaySarma.Sdk.DataSources.AzureTables.Attributes;

using System;
using System.Collections.Generic;
using System.Reflection;

namespace SujaySarma.Sdk.DataSources.AzureTables.PrivateReflector
{
    /// <summary>
    /// Class inspects a type and loads up interesting metadata
    /// </summary>
    internal static class TypeInspector
    {
        /// <summary>
        /// Inspect a class for Azure Tables
        /// </summary>
        /// <typeparam name="ClassT">Type of business class to be stored into Azure Tables</typeparam>
        /// <returns>Reflected class metadata</returns>
        public static ClassInformation? InspectForAzureTables<ClassT>()
            where ClassT : class
        {
#pragma warning disable CS8604 // Possible null reference argument.
            Type classType = typeof(ClassT);
            ClassInformation? objectMetadata = Cache.TryGet(classType.FullName);
            if (objectMetadata != null)
            {
                // cache hit
                return objectMetadata;
            }
#pragma warning restore CS8604 // Possible null reference argument.

            TableAttribute? tableAttribute = classType.GetCustomAttribute<TableAttribute>(true);
            if (tableAttribute == null)
            {
                return null;
            }

            List<Property> properties = new List<Property>();
            List<Field> fields = new List<Field>();
            foreach (MemberInfo member in classType.GetMembers(MEMBER_SEARCH_FLAGS))
            {
                object[] memberAttributes = member.GetCustomAttributes(true);
                if ((memberAttributes == null) || (memberAttributes.Length == 0))
                {
                    continue;
                }

                bool hasAttribute = false;
                foreach (object attribute in memberAttributes)
                {
                    if ((attribute is PartitionKeyAttribute) || (attribute is RowKeyAttribute) || (attribute is TableColumnAttribute))
                    {
                        hasAttribute = true;
                        break;
                    }
                }

                if (!hasAttribute)
                {
                    continue;
                }

                switch (member.MemberType)
                {
                    case MemberTypes.Field:
                        FieldInfo? fi = member as FieldInfo;
                        if (fi != null)
                        {
                            fields.Add(new Field(fi));
                        }
                        break;

                    case MemberTypes.Property:
                        PropertyInfo? pi = member as PropertyInfo;
                        if (pi != null)
                        {
                            properties.Add(new Property(pi));
                        }
                        break;
                }
            }

            if ((properties.Count == 0) && (fields.Count == 0))
            {
                return null;
            }

#pragma warning disable CS8604 // Possible null reference argument.
            objectMetadata = new ClassInformation(classType.Name, classType.FullName, tableAttribute, properties, fields);
            Cache.TrySet(objectMetadata, classType.FullName);
#pragma warning restore CS8604 // Possible null reference argument.

            return objectMetadata;
        }


        /// <summary>
        /// A cache of objects created by the <see cref="TypeInspector"/> class
        /// </summary>
        private static class Cache
        {
            private static readonly Dictionary<string, ClassInformation> cache;
            private static readonly object cacheAccessLock = new object();

            static Cache()
            {
                cache = new Dictionary<string, ClassInformation>();
            }

            /// <summary>
            /// Add an item to cache if it is not already in it
            /// </summary>
            /// <param name="info">Item to add</param>
            /// <param name="keyName">The key name of the object</param>
            public static void TrySet(ClassInformation info, string keyName)
            {
                lock (cacheAccessLock)
                {
                    if (!cache.ContainsKey(keyName))
                    {
                        cache.Add(keyName, info);
                    }
                }
            }

            /// <summary>
            /// Fetch an item from cache
            /// </summary>
            /// <param name="keyName">Key name of object</param>
            /// <returns>Cached information or NULL</returns>
            public static ClassInformation? TryGet(string keyName)
            {
                if (!cache.TryGetValue(keyName, out ClassInformation? info))
                {
                    return null;
                }

                return info;
            }
        }

        private static readonly BindingFlags MEMBER_SEARCH_FLAGS = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static;
    }
}
