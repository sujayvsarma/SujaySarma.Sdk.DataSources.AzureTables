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
            Type classType = typeof(ClassT);
            string cacheKeyName = classType.FullName ?? classType.Name;
            ClassInformation? objectMetadata = Cache.TryGet(cacheKeyName);
            if (objectMetadata != null)
            {
                // cache hit
                return objectMetadata;
            }

            TableAttribute? tableAttribute = classType.GetCustomAttribute<TableAttribute>(true);
            if (tableAttribute == null)
            {
                return null;
            }

            List<Property> properties = new List<Property>();
            List<Field> fields = new List<Field>();

            bool hasPartitionKey = false, hasRowKey = false, hasETag = false, hasTimestamp = false;

            foreach (MemberInfo member in classType.GetMembers(MEMBER_SEARCH_FLAGS))
            {
                object[] memberAttributes = member.GetCustomAttributes(true);
                if ((memberAttributes == null) || (memberAttributes.Length == 0))
                {
                    continue;
                }

                foreach (object attribute in memberAttributes)
                {
                    if (attribute is ETagAttribute)
                    {
                        if (hasETag)
                        {
                            throw new InvalidOperationException($"'{cacheKeyName}' has multiple ETag properties defined.");
                        }

                        hasETag = true;
                    }

                    if (attribute is PartitionKeyAttribute)
                    {
                        if (hasPartitionKey)
                        {
                            throw new InvalidOperationException($"'{cacheKeyName}' has multiple PartitionKey properties defined.");
                        }

                        hasPartitionKey = true;
                    }

                    if (attribute is RowKeyAttribute)
                    {
                        if (hasRowKey)
                        {
                            throw new InvalidOperationException($"'{cacheKeyName}' has multiple RowKey properties defined.");
                        }

                        hasRowKey = true;
                    }

                    if (attribute is TimestampAttribute)
                    {
                        // since we never read this back into the TableEntity, there can be as many of these as the developer wants :)
                        hasTimestamp = true;
                    }
                }

                if (!(hasPartitionKey || hasRowKey))
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

            if ((properties.Count == 0) && (fields.Count == 0) || (! hasPartitionKey) || (! hasRowKey))
            {
                return null;
            }

            objectMetadata = new ClassInformation(classType.Name, cacheKeyName, tableAttribute, properties, fields)
            {
                // without these two, we don't get here!
                HasPartitionKey = true,
                HasRowKey = true,

                HasETag = hasETag,
                HasTimestamp = hasTimestamp
            };


            Cache.TrySet(objectMetadata, cacheKeyName);

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
