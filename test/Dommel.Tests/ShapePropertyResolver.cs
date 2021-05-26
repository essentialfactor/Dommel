using System;
using System.Collections.Generic;
using System.Text;

namespace Dommel.Tests
{
    public class ShapePropertyResolver : DefaultPropertyResolver
    {
        protected override HashSet<Type> PrimitiveTypes
        {
            get
            {
                var types = base.PrimitiveTypes;
                types.Add(typeof(Microsoft.SqlServer.Types.SqlGeography));
                return types;
            }
        }
    }
}
