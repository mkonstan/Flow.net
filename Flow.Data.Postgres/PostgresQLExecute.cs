using Npgsql;
using System.Data;

namespace Flow.Data.Postgres
{
    public class PostgresQLExecute : DbExecute
    {
        public CommandType CommandType { get; set; } = CommandType.Text;

        protected override IDbConnection CreateConnection(string connectionString) { return new NpgsqlConnection(connectionString); }

        protected override CommandType? GetCommandType() => CommandType;
    }
}
