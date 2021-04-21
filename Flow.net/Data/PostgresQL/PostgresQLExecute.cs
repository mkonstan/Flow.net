using Npgsql;
using System;
using System.Data;
using System.Linq;

namespace Flow.Data.PostgresQL
{
    public class PostgresQLExecute : DbExecute
    {
        public CommandType CommandType { get; set; } = CommandType.Text;

        protected override IDbConnection CreateConnection(string connectionString) { return new NpgsqlConnection(connectionString); }

        protected override CommandType? GetCommandType() => CommandType;
    }
}
