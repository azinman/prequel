package net.noerd.prequel

import scala.collection.mutable.ArrayBuffer

import java.net.URL
import java.math.BigDecimal
import java.util.UUID

import java.sql.Array
import java.sql.Connection
import java.sql.Statement
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp
import java.sql.Types

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import com.typemapper.postgres.HStore

import net.noerd.prequel.RichConnection.conn2RichConn

object NullArray
object NullBigDecimal
object NullBoolean
object NullDateTime
object NullDouble
object NullFloat
object NullInt
object NullLong
object NullString
object NullShort
object NullURL
object NullHStore
object NullUUID

/**
 * A Transaction is normally created by the InTransaction object and can be
 * used to execute one or more queries against the database. Once the block
 * passed to InTransaction is succesfully executed the transaction is auto-
 * matically committed. And if some exception is throws during execution the
 * transaction is rollbacked.
 *
 * @throws SQLException all methods executing queries will throw SQLException
 *         if the query was not properly formatted or something went wrong in
 *         the database during execution.
 *
 * @throws IllegalFormatException: Will be throw by all method if the format
 *         string is invalid or if there is not enough parameters.
 */
class Transaction( val connection: Connection ) {
    var lastGeneratedKeys:List[Long] = Nil

    /**
     * Returns all records returned by the query after being converted by the
     * given block. All objects are kept in memory to this method is no suited
     * for very big result sets. Use selectAndProcess if you need to process
     * bigger datasets.
     *
     * @param sql query that should return records
     * @param params are the optional parameters used in the query
     * @param block is a function converting the row to something else
     */
    def select[ T ]( sql: String, params: Any* )( block: ResultSetRow => T ): Seq[ T ] = {
        val results = new ArrayBuffer[ T ]
        _selectIntoBuffer( Some( results ), sql, params.toSeq )( block )
        results
    }

    /**
     * Executes the query and passes each row to the given block. This method
     * does not keep the objects in memory and returns Unit so the row needs to
     * be fully processed in the block.
     *
     * @param sql query that should return records
     * @param params are the optional parameters used in the query
     * @param block is a function fully processing each row
     */
    def selectAndProcess( sql: String, params: Any* )( block: ResultSetRow => Unit ): Unit = {
        _selectIntoBuffer( None, sql, params.toSeq )( block )
    }


    /**
     * Returns the first record returned by the query after being converted by the
     * given block. If the query does not return anything None is returned.
     *
     * @param sql query that should return records
     * @param params are the optional parameters used in the query
     * @param block is a function converting the row to something else
     */
    def selectHeadOption[ T ]( sql: String, params: Any* )( block: ResultSetRow => T ): Option[ T ] = {
        select( sql, params.toSeq: _* )( block ).headOption
    }

    /**
     * Return the head record from a query that must be guaranteed to return at least one record.
     * The query may return more records but those will be ignored.
     *
     * @param sql is a query that must return at least one record
     * @param params are the optional parameters of the query
     * @param block is a function converting the returned row to something useful.
     * @throws NoSuchElementException if the query did not return any records.
     */
    def selectHead[ T ]( sql: String, params: Any* )( block: ResultSetRow => T ): T = {
        select( sql, params.toSeq: _* )( block ).head
    }


    /**
     * Executes the given query and returns the number of affected records.
     *
     * For null values, send the Class object instead. Null itself is never allowed.
     *
     * @param sql query that must not return any records
     * @param params are the optional parameters used in the query
     * @return the number of affected records
     */
    def executeUpdate( sql: String, params: Any* ): Int = {
        connection.usingPreparedStatement(sql) { statement =>
            Transaction.setParameters(statement, params)
            var num = statement.executeUpdate()
            lastGeneratedKeys = Nil
            if (num > 0) {
                var rs:ResultSet = null
                try {
                    rs = statement.getGeneratedKeys
                    if (rs != null) {
                        while (rs.next) {
                            lastGeneratedKeys = lastGeneratedKeys ::: rs.getLong(1) :: Nil
                        }
                    }
                } catch {
                    case e:Throwable =>
                        println("Got sql exception in update for generated keys:" + e)
                        e.printStackTrace
                        try { rollback } catch { case _ => }
                } finally {
                    if (rs != null) try { rs.close } catch { case _ => }
                }
            }
            num
        }
    }

    /**
     * Will pass a PreparedStatement to the given block. This block
     * may add parameters to the statement and execute it multiple times.
     * The statement will be automatically closed onced the block returns.
     *
     * Example:
     *     tx.executeBatch( "insert into foo values(?)" ) { statement =>
     *         items.foreach { statement.executeWith( _ ) }
     *     }
     *
     * @return the result of the block
     * @throws SQLException if the query is missing parameters when executed
     *         or if they are of the wrong type.
     */
    def executeBatch[ T ]( sql: String, generateKeys: Boolean = false )( block: (PreparedStatement) => T ): T = {
        connection.usingPreparedStatement( sql, generateKeys )( block )
    }

    /**
     * Rollbacks the Transaction.
     *
     * @throws SQLException if transaction could not be rollbacked
     */
    def rollback(): Unit = connection.rollback()

    /**
     * Commits all changed done in the Transaction.
     *
     * @throws SQLException if transaction could not be committed.
     */
    def commit(): Unit = connection.commit()

    private def _selectIntoBuffer[ T ](
        buffer: Option[ ArrayBuffer[T] ],
        sql: String, params: Seq[ Any ]
    )( block: ( ResultSetRow ) => T ): Unit = {
        connection.usingPreparedStatement(sql) { statement =>
            Transaction.setParameters(statement, params)
            val rs = statement.executeQuery()
            val append = buffer.isDefined

            while( rs.next ) {
                val value = block( ResultSetRow( rs ) )
                if( append ) buffer.get.append( value )
            }
        }
    }
}

object Transaction {
    def apply( conn: Connection ) = new Transaction( conn )

    def setParameters(statement:PreparedStatement, params:Seq[Any]) = {
        var position = 0
        params.foreach { param:Any =>
            position += 1
            param match {
                case value:java.sql.Array => statement.setArray(position, value)
                case value:BigDecimal => statement.setBigDecimal(position, value)
                case value:Boolean => statement.setBoolean(position, value)
                case value:DateTime => statement.setTimestamp(position, new Timestamp(value.getMillis), value.toGregorianCalendar)
                case value:Double => statement.setDouble(position, value)
                case value:Float => statement.setFloat(position, value)
                case value:Int => statement.setInt(position, value)
                case value:Long => statement.setLong(position, value)
                case value:String => statement.setString(position, value)
                case value:Short => statement.setShort(position, value)
                case value:URL => statement.setURL(position, value)
                case value:HStore => statement.setObject(position, value)
                case value:UUID => statement.setObject(position, value)
                case NullArray => statement.setNull(position, Types.ARRAY)
                case NullBigDecimal => statement.setNull(position, Types.NUMERIC)
                case NullBoolean => statement.setNull(position, Types.BOOLEAN)
                case NullDateTime => statement.setNull(position, Types.TIMESTAMP)
                case NullDouble => statement.setNull(position, Types.DOUBLE)
                case NullFloat => statement.setNull(position, Types.FLOAT)
                case NullInt => statement.setNull(position, Types.INTEGER)
                case NullLong => statement.setNull(position, Types.INTEGER)
                case NullString => statement.setNull(position, Types.VARCHAR)
                case NullShort => statement.setNull(position, Types.TINYINT)
                case NullURL => statement.setNull(position, Types.VARCHAR)
                case NullHStore => statement.setNull(position, Types.OTHER)
                case NullUUID => statement.setNull(position, Types.OTHER)
            }
        }
    }
}