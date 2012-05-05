package net.noerd.prequel

import java.math.BigDecimal
import java.util.Date
import java.util.UUID

import java.sql.Timestamp
import java.sql.ResultSet
import java.sql.ResultSetMetaData

import scala.collection.mutable.ArrayBuffer

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Duration
import com.typemapper.postgres.HStore

/**
 * Wraps a ResultSet in a row context. The ResultSetRow gives access
 * to the current row with no possibility to change row. The data of
 * the row can be accessed though the next<Type> methods which return
 * the optional value of the next column.
 */
class ResultSetRow( val rs: ResultSet ) {
    /** Maintain the current position. */
    private var position = 0

    def nextBoolean: Option[ Boolean ] = nextValueOption( rs.getBoolean )
    def nextInt: Option[ Int ] = nextValueOption( rs.getInt )
    def nextLong: Option[ Long ] = nextValueOption( rs.getLong )
    def nextFloat: Option[ Float ] = nextValueOption( rs.getFloat )
    def nextDouble: Option[ Double ] = nextValueOption( rs.getDouble )
    def nextString: Option[ String ] = nextValueOption( rs.getString )
    def nextDate: Option[ Date ] =  nextValueOption( rs.getTimestamp )
    def nextObject: Option[ AnyRef ] = nextValueOption( rs.getObject )
    def nextArray: Option[ java.sql.Array ] = nextValueOption( rs.getArray )
    def nextBigDecimal: Option[ BigDecimal ] = nextValueOption( rs.getBigDecimal )
    // Extended objects for postgres
    def nextUUID: Option[  UUID ] = nextValueOption( rs.getObject ) match {
        case Some(uuid:UUID) => Some(uuid)
        case None => None
    }
    def nextHStore: Option[ HStore ] = nextValueOption( rs.getObject ) match {
        case Some(hstore:HStore) => Some(hstore)
        case None => None
    }
    def nextDateTimeUTC: Option[ DateTime ] = nextValueOption ( rs.getTimestamp ) match {
        case Some(timestamp:Timestamp) => Some(new DateTime( timestamp.getTime, DateTimeZone.UTC ))
        case None => None
    }

    def columnNames: Seq[ String ]= {
        val columnNames = ArrayBuffer.empty[ String ]
        val metaData = rs.getMetaData
        for(index <- 0.until( metaData.getColumnCount ) ) {
            columnNames += metaData.getColumnName( index + 1 ).toLowerCase
        }
        columnNames
    }

    private def incrementPosition = {
        position = position + 1
    }

    private def nextValueOption[T]( f: (Int) => T ): Option[ T ] = {
        incrementPosition
        val value = f( position )
        if( rs.wasNull ) None
        else Some( value )
    }
}

object ResultSetRow {
    def apply( rs: ResultSet ): ResultSetRow = {
        new ResultSetRow( rs )
    }
}
