using System.Text.RegularExpressions;

namespace SujaySarma.Sdk.DataSources.AzureTables.Utility
{
    /// <summary>
    /// Validate table keys
    /// </summary>
    public static class TableKeyValidator
    {
        /// <summary>
        /// Check if the proposedValue is valid as a partition/row key
        /// </summary>
        /// <param name="proposedValue">Value to check (string)</param>
        /// <returns>True if value can be used</returns>
        public static bool IsValid(string proposedValue)
        {
            return (!TableKeysValidationRegEx.IsMatch(proposedValue));
        }

        private static readonly Regex TableKeysValidationRegEx = new Regex(@"[\\\\#%+/?\u0000-\u001F\u007F-\u009F]");
    }
}
