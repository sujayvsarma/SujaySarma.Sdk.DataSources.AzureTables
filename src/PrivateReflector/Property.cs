using System;
using System.Reflection;

namespace SujaySarma.Sdk.DataSources.AzureTables.PrivateReflector
{
    /// <summary>
    /// Structure to cache metadata for the properties in an object. Also caches information 
    /// about their get/set methods.
    /// </summary>
    internal class Property : FieldOrPropertyBase
    {

        /// <summary>
        /// Flag indicating if the member can be read (has a Getter)
        /// </summary>
        public bool CanRead { get; private set; }

        /// <summary>
        /// The MethodInfo for the member's GET accessor
        /// </summary>
        public MethodInfo? GetMethod { get; private set; }

        /// <summary>
        /// The MethodInfo for the member's SET accessor
        /// </summary>
        public MethodInfo? SetMethod { get; private set; }

        /// <summary>
        /// Initialize the structure for a Property
        /// </summary>
        /// <param name="reflectedProperty">Property to import</param>
        public Property(System.Reflection.PropertyInfo reflectedProperty)
            : base(reflectedProperty)
        {
            CanRead = reflectedProperty.CanRead;
            GetMethod = (CanRead ? reflectedProperty.GetGetMethod(true) : null);
            SetMethod = (CanWrite ? reflectedProperty.GetSetMethod(true) : null);
        }


        /// <summary>
        /// Read the value from the property or field and return it
        /// </summary>
        /// <typeparam name="ObjType">Data type of the object to read the property or field of</typeparam>
        /// <param name="obj">The object instance to read the value from</param>
        /// <returns>The value</returns>
        public override object? Read<ObjType>(ObjType obj)
        {
            if (obj == null)
            {
                throw new ArgumentNullException("Instance of object is NULL.");
            }

            if (!CanRead)
            {
                return null;
            }

            return GetMethod?.Invoke(obj, FLAGS_READ_WRITE, null, null, null);
        }

        /// <summary>
        /// Writes the provided value to the object instance - WITHOUT type conversion.
        /// </summary>
        /// <typeparam name="ObjType">Data type of the object to read the property/field of</typeparam>
        /// <param name="obj">The object instance to write the value to</param>
        /// <param name="value">Value to write out</param>
        public override void Write<ObjType>(ObjType obj, object? value)
        {
            if (obj == null)
            {
                throw new ArgumentNullException("Instance of object is NULL.");
            }

            if (SetMethod == null)
            {
                // property has no SET!
                return;
            }

            if (value == null)
            {
                // Trying to set NULL into a non-Nullable type
                return;
            }

            SetMethod?.Invoke(obj, FLAGS_READ_WRITE, null, new object[] { value }, null);
        }
    }
}
