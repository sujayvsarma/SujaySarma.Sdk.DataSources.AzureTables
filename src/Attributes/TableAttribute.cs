using System;

namespace SujaySarma.Sdk.DataSources.AzureTables.Attributes
{
    /// <summary>
    /// Provide name of the table the data for the class is stored in.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface, AllowMultiple = false, Inherited = true)]
    public sealed class TableAttribute : Attribute
    {

        /// <summary>
        /// Name of the table
        /// </summary>
        public string TableName { get; private set; }

        /// <summary>
        /// If set, we use soft-delete by setting the IsDeleted flag to true.
        /// </summary>
        public bool UseSoftDelete { get; set; }

        /// <summary>
        /// Provides information about the table used to contain the data for an object.
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        public TableAttribute(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new ArgumentNullException();
            }

            TableName = tableName;
        }

    }
}
