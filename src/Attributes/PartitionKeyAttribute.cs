using System;

namespace SujaySarma.Sdk.DataSources.AzureTables.Attributes
{
    /// <summary>
    /// Marks the property or field as a partition key for the table.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false, Inherited = true)]
    public sealed class PartitionKeyAttribute : Attribute
    {
        // Nothing to be done here
    }
}
