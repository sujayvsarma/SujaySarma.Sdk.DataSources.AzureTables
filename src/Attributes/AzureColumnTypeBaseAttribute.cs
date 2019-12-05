using System;

namespace SujaySarma.Sdk.DataSources.AzureTables.Attributes
{
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true, Inherited = true)]
    public class AzureColumnTypeBaseAttribute : Attribute
    {
        // nothing to do here
    }
}
