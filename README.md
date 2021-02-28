# SujaySarma.Sdk.DataSources.AzureTables
The revamped SujaySarma.Sdk.DataSources.AzureTables library. 

## Dependencies
Other libraries: `Microsoft.Azure.Cosmos.Table` version 1.0.8+

## Compatibility

.Net Version|Compat
------------|---------
.Net Framework|[ ] No
.Net Core|[X] .Net Core 3.1
.Net 5.0|[X] Library builds on 5.0
.Net Standard|[X] .Net Standard 2.0


## Json Serialization
This package uses a mix of `System.Text.Json` (.NET Core 3.0) and the `Newtonsoft.Json` libraries. This is because there is atleast ONE place in code where the Newtonsoft library is more reliable than the .NET Core library. When and if this changes, we will move to a single consolidated parser.

## Usage
Since you are a developer, I will keep this simple. If you have questions, file an issue and I will respond there.

**In your Business Class**

Import these two namespaces:

```
using SujaySarma.Sdk.DataSources.AzureTables;
using SujaySarma.Sdk.DataSources.AzureTables.Attributes;
```

Decorate your class thus:

```
[Table("UserAccount", UseSoftDelete = true)]
public class UserAccount
```

The first parameter to the Table attribute is the name of the table. In the above example, the data from the `UserAccount` business class is stored into a **table** called `UserAccount` on Azure Table Storage. The second parameter (`UseSoftDelete`) is `false` by default -- setting it to `true` will mean that when you call a `Delete` on this class, data is actually soft-deleted -- you will see a `Boolean` column named `IsDeleted` set to `true` in the Azure Table.

Now you need a `PartitionKey` and a `RowKey`. Simply decorate the corresponding properties thus:

```
[PartitionKey]
private string AuthProvider { get; set; }

[RowKey]
private Guid UserId { get; set; }
```

As long as the data that you store is "url-able" and matches the conditions for partition and row keys as set forth by Azure Table Service, we can use it -- so you can have String, Boolean, Guid, numeric,... data types. **Please don't use complex objects (such as other classes) because the engine will serialize it to `string`, causing problems with ATS.**

Decorate other properties with `TableColumn` attributes providing the name of the column:

```
[TableColumn("Password")]
private string Password;
```

Note that the table columns may be class properties or fields. They may be of any visibility scope (public, private, internal, ...). You can have columns that only store data (provide only a GET on the property) or only load data (provide only a SET on the property). 

**Data modification methods:**
You first need to get an instance of the AzureTablesDataSource. You may write a small function that does this in common and uses caching so that you do not need to go through this on every call:

You first need to call the `GetTableName()` (static) method in `AzureTablesDataSource` class to get the name of the table.
```
string tableName = AzureTablesDataSource.GetTableName<UserAccount>();
```

Now initialize an instance of `AzureTablesDataSource` with your connection string to Azure Storage and the above table name:
```
AzureTablesDataSource ds = new  AzureTablesDataSource(connectionString, tableName);
```

Now you may call:

Data Modification|Call
-----------------|----------
Insert|`ds.Insert(new [] { this });`
Update|`ds.Update(new [] { this });`
Delete|`ds.Delete(new [] { this });`

The `Insert`, `Update` and `Delete` methods take an IEnumerable<T>, allowing you to batch the operations. 
  
Here is a nice little helper class I use to get the `AzureTablesDataSource` instances and cache them:

```
using SujaySarma.Sdk.Core;
using SujaySarma.Sdk.DataSources.AzureTables;

using System.Collections.Generic;

namespace Extensions
{
    /// <summary>
    /// Provides access to the data sources
    /// </summary>
    internal static class DataLayer
    {

        /// <summary>
        /// Gets an initialized reference to a table data source
        /// </summary>
        /// <typeparam name="T">Type of business object to get table for</typeparam>
        /// <returns>AzureTablesDataSource ready to use</returns>
        public static AzureTablesDataSource GetDataSource<T>()
            where T : class
        {
            string tableName = AzureTablesDataSource.GetTableName<T>();
            if (! tableDataSources.ContainsKey(tableName))
            {
                lock (concurrencyLock)
                {
                    tableDataSources.Add(
                            tableName,
                            new AzureTablesDataSource(connectionString, tableName)
                        );
                }
            }

            return tableDataSources[tableName];
        }


        static DataLayer()
        {
            connectionString = ConfigurationManager.Settings.ConnectionStrings["AzureTableStorage"];
            tableDataSources = new Dictionary<string, AzureTablesDataSource>();
            concurrencyLock = new object();
        }

        private static readonly string connectionString;
        private static readonly Dictionary<string, AzureTablesDataSource> tableDataSources;
        private static readonly object concurrencyLock;

    }
}
```

With this helper class, my call to `Insert` a `UserAccount` becomes: 

```
DataLayer.GetDataSource<UserAccount>().Insert(new [] { this });
```

Hope you enjoy using this library!
