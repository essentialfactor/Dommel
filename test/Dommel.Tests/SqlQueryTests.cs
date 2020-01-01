using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using static Dommel.DommelMapper;

namespace Dommel.Tests
{
    public class SqlQueryTests
    {
        private readonly SqlQuery<Product> _sqlQuery = new SqlQuery<Product>(new SqlServerSqlBuilder());

        [Fact]
        public void MultipleSeparateProperties()
        {
            var sql = _sqlQuery
                .Select<Product>(nameof(Product.Id))
                .Select<Product>(nameof(Product.Name))
                .ToSql();
            AssertQueryMatches("SELECT [Products].[Id], [Products].[Name] FROM [Products]", sql);
        }

        [Fact]
        public void CreateDefaultJoin()
        {
            var sql = _sqlQuery
                .Select<Product>(nameof(Product.Id))
                .InnerJoin<Category>()
                .Select<Category>(nameof(Category.Id), nameof(Category.Name))
                .SplitOn(nameof(Category.Id), typeof(Category))
                .ToSql();
            AssertQueryMatches("SELECT [Products].[Id], [Categories].[Id], [Categories].[Name] FROM [Products] INNER JOIN [Categories] ON [Categories].[Id] = [Products].[CategoryId]", sql);
        }

        private void AssertQueryMatches(string expected, string actual)
        {
            var removeLineBreaks = System.Text.RegularExpressions.Regex.Replace(actual, @"\r\n?|\n", " ");
            removeLineBreaks = removeLineBreaks.Replace(" ,", ",");
            Assert.Equal(expected, removeLineBreaks.Trim(), ignoreWhiteSpaceDifferences: true);
        }
    }
}
