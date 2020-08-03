﻿using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;

namespace Dommel
{
    /// <summary>
    /// Represents the column of an entity.
    /// </summary>
    public class ColumnPropertyInfo
    {
        /// <summary>
        /// Initializes a new <see cref="ColumnPropertyInfo"/> instance from the
        /// specified <see cref="PropertyInfo"/> instance.
        /// </summary>
        /// <param name="property">
        /// The property which represents the database column. The <see cref="DatabaseGeneratedOption"/> is
        /// determined from the <see cref="DatabaseGeneratedAttribute"/> option specified on
        /// the property. Defaults to <see cref="DatabaseGeneratedOption.Identity"/> when <paramref name="isKey"/>
        /// is <c>true</c>; otherwise, <see cref="DatabaseGeneratedOption.None"/>.
        /// </param>
        /// <param name="isKey">Indicates whether a property is a key column.</param>
        public ColumnPropertyInfo(PropertyInfo property, bool isKey = false)
        {
            Property = property ?? throw new ArgumentNullException(nameof(property));
            GeneratedOption = property.GetCustomAttribute<DatabaseGeneratedAttribute>()?.DatabaseGeneratedOption
                ?? (isKey ? DatabaseGeneratedOption.Identity : DatabaseGeneratedOption.None);
        }

        /// <summary>
        /// Initializes a new <see cref="ColumnPropertyInfo"/> instance from the
        /// specified <see cref="PropertyInfo"/> instance using the specified
        /// <see cref="DatabaseGeneratedOption"/>.
        /// </summary>
        /// <param name="property">The property which represents the database column.</param>
        /// <param name="generatedOption">
        /// The <see cref="DatabaseGeneratedOption"/> which specifies whether the value of
        /// the column this property represents is generated by the database.
        /// </param>
        public ColumnPropertyInfo(PropertyInfo property, DatabaseGeneratedOption generatedOption)
        {
            Property = property ?? throw new ArgumentNullException(nameof(property));
            GeneratedOption = generatedOption;
        }

        /// <summary>
        /// Gets a reference to the <see cref="PropertyInfo"/> instance.
        /// </summary>
        public PropertyInfo Property { get; }

        /// <summary>
        /// Gets the <see cref="DatabaseGeneratedOption"/> which specifies whether the value of
        /// the column this property represents is generated by the database.
        /// </summary>
        public DatabaseGeneratedOption GeneratedOption { get; }

        /// <summary>
        /// Gets a value indicating whether this key property's value is generated by the database.
        /// </summary>
        public bool IsGenerated => GeneratedOption != DatabaseGeneratedOption.None;
    }
}
