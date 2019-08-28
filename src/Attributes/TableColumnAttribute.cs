using System;

namespace SujaySarma.Sdk.DataSources.AzureTables.Attributes
{
    /// <summary>
    /// Provide the data table column name the value for this property or field is stored in or retrieved from.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false, Inherited = true)]
    public sealed class TableColumnAttribute : Attribute
    {
        /// <summary>
        /// Name of the column
        /// </summary>
        public string ColumnName { get; private set; }

        /// <summary>
        /// Provides information about the table column used to contain the data for an object.
        /// </summary>
        /// <param name="columnName">Name of the column</param>
        public TableColumnAttribute(string columnName)
        {
            if (string.IsNullOrWhiteSpace(columnName))
            {
                throw new ArgumentNullException();
            }

            ColumnName = columnName;
        }
    }
}
