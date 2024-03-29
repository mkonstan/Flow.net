﻿using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Flow.Data
{
    public abstract class DbExecute : PipelineAction, IDbAction
    {
        public string ConnectionString { get; set; }

        public string Query { get; set; }

        public int? CommandTimeout { get; set; }

        public IEnumerable<IQueryParameterBuilder> Parameters { get; set; } = new List<IQueryParameterBuilder>();

        protected abstract IDbConnection CreateConnection(string connectionString);

        protected virtual CommandType? GetCommandType() => null;

        protected override async Task<IPayload> DefaultHandlerAsync(IExecutionContext context, IPayload input)
        {
            using (var conn = CreateConnection(Format(ConnectionString, context, input, this)))
            {
                var ps = new Dapper.DynamicParameters();
                foreach (var builder in Parameters)
                {
                    var p = builder.Create(context, input, this);
                    ps.Add(p.Name, p.Value, p.Type, p.ParameterDirection, p.Size);
                }
                var result = await conn.ExecuteAsync(
                    Format(Query, context, input, this),
                    ps,
                    null,
                    CommandTimeout ?? 0,
                    GetCommandType());
                return new ValueResult<int>(result);
            }
        }
    }
}
