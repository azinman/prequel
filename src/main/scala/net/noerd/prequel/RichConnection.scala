package net.noerd.prequel

import java.sql.Connection
import java.sql.Statement
import java.sql.PreparedStatement

import java.sql.Statement.RETURN_GENERATED_KEYS
import java.sql.Statement.NO_GENERATED_KEYS

/**
 * Private class providing methods for using Statements and
 * ReusableStatements.
 */
private[prequel] class RichConnection( val wrapped: Connection ) {
    /**
     * Creates a new statement executes the given block with it.
     * The statement is automatically closed once the block has finished.
     */
    def usingStatement[ T ]( block: (Statement) => T ): T = {
        val statement = wrapped.createStatement

        try {
            block( statement )
        }
        finally {
            // This also closes the resultset
            statement.close()
        }
    }

    /**
     * Prepares the sql query and executes the given block with it.
     * The statement is automatically closed once the block has finished.
     */
    def usingPreparedStatement[ T ](
        sql: String,
        generateKeys: Boolean = false
    )
    ( block: (PreparedStatement) => T ): T = {
        val keysOption = (
            if( generateKeys ) RETURN_GENERATED_KEYS
            else NO_GENERATED_KEYS
        )
        val statement = wrapped.prepareStatement( sql, keysOption )

        try {
            block( statement )
        }
        finally {
            statement.close()
        }
    }
}

private[prequel] object RichConnection {

    implicit def conn2RichConn( conn: Connection ): RichConnection = {
        new RichConnection( conn )
    }
}
