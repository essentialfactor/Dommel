using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Dommel
{
    /// <summary>
    /// Default implemenation of the <see cref="IPropertyResolver"/> interface.
    /// </summary>
    public class DefaultPropertyResolver : IPropertyResolver
    {
        private static readonly HashSet<Type> PrimitiveTypesSet = new HashSet<Type>
            {
                typeof(object),
                typeof(string),
                typeof(Guid),
                typeof(decimal),
                typeof(double),
                typeof(float),
                typeof(DateTime),
                typeof(DateTimeOffset),
                typeof(TimeSpan),
                typeof(byte[])
            };

        /// <inheritdoc/>
        public virtual IEnumerable<PropertyInfo> ResolveProperties(Type type, IEnumerable<string>? properties = default, bool fullyQualified = false)
        {
            var includedProperties = type.GetRuntimeProperties();

            if (properties != default)
            {
                includedProperties = includedProperties.Where(x => properties.Contains(x.Name, StringComparer.OrdinalIgnoreCase));
            }

            return FilterComplexTypes(includedProperties).Where(p => p.GetSetMethod() is object && !p.IsDefined(typeof(IgnoreAttribute)));
        }

        /// <summary>
        /// Gets a collection of types that are considered 'primitive' for Dommel but are not for the CLR.
        /// Override this to specify your own set of types.
        /// </summary>
        protected virtual HashSet<Type> PrimitiveTypes => PrimitiveTypesSet;

        /// <summary>
        /// Filters the complex types from the specified collection of properties.
        /// </summary>
        /// <param name="properties">A collection of properties.</param>
        /// <returns>The properties that are considered 'primitive' of <paramref name="properties"/>.</returns>
        protected virtual IEnumerable<PropertyInfo> FilterComplexTypes(IEnumerable<PropertyInfo> properties)
        {
            foreach (var property in properties)
            {
                var type = property.PropertyType;
                type = Nullable.GetUnderlyingType(type) ?? type;
                if (type.GetTypeInfo().IsPrimitive || type.GetTypeInfo().IsEnum || PrimitiveTypes.Contains(type))
                {
                    yield return property;
                }
            }
        }
    }
}
