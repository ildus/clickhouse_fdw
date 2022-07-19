#pragma once

namespace clickhouse {

    /// Types of data which are sent by the server
    namespace ServerCodes {
        enum {
            Hello       = 0,    /// Name, version, revision
            Data        = 1,    /// Block with compression or without
            Exception   = 2,    /// Exception while querying
            Progress    = 3,    /// Execution progress: rows and bytes read.
            Pong        = 4,    /// Answer on Ping request.
            EndOfStream = 5,    /// All blocks were transferred.
            ProfileInfo = 6,    /// A package with profiling data.
            Totals      = 7,    /// Block of data with totals, with compression or without.
            Extremes    = 8,    /// Block of data with extremes, with compression or without.
            TablesStatusResponse = 9,    /// A response to TablesStatus request.
            Log			 = 10,   /// System logs of the query execution
            TableColumns         = 11    /// Columns' description for default values calculation
        };
    }

    /// Types of data which are sent by the client
    namespace ClientCodes {
        enum {
            Hello       = 0,    /// Name, version, revision, default database
            Query       = 1,    /** Id of query, query settings,
                                  * information of stage to run the query until,
                                  * use compression or not, query (without INSERT data).
                                  */
            Data        = 2,    /// Block of data, with compression or without
            Cancel      = 3,    /// Cancel the request
            Ping        = 4,    /// Check connection with the server
        };
    }

    /// Use compression or not
    namespace CompressionState {
        enum {
            Disable     = 0,
            Enable      = 1,
        };
    }

    /// Query stages
    namespace Stages {
        enum {
            Complete    = 2,
        };
    }
}
