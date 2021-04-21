using Npgsql;
using System;
using System.Data;
using System.Linq;

namespace Flow.Data.SqlServer
{
    public class SqlServerExecute : DbExecute
    {
        public CommandType CommandType { get; set; } = CommandType.Text;

        protected override IDbConnection CreateConnection(string connectionString) { return new System.Data.SqlClient.SqlConnection(connectionString); }

        protected override CommandType? GetCommandType() => CommandType;
    }

    public class SqlServerQuery : DbQuery
    {
        public CommandType CommandType { get; set; } = CommandType.Text;

        protected override IDbConnection CreateConnection(string connectionString) { return new System.Data.SqlClient.SqlConnection(connectionString); }

        protected override CommandType? GetCommandType() => CommandType;
    }
}
